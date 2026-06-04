# ARCHITECTURE.md

Reference document for the internals of rtpigate — a receive-only APRS iGate for the KA9Q-Radio backend. Describes the implementation as it currently stands.

## System Overview

rtpigate subscribes to a KA9Q-Radio channel's **RTP audio multicast group**, demodulates the 1200-baud AFSK and parses AX.25/APRS entirely in-process, then filters and gates qualifying packets to APRS-IS. A web dashboard receives real-time updates over Server-Sent Events.

```
┌─────────────┐  RTP audio   ┌──────────────────┐  broadcast   ┌──────────┐  broadcast   ┌───────────┐
│  KA9Q-Radio │──(PCM/UDP ──>│     ka9q.rs      │──(DataItem)─>│  sse.rs  │──(SSEEvent)─>│  axum     │──> Browser
│  (SDR)      │   multicast) │  aprs-rtp +      │              │          │              │  /api/sse │
│             │              │  aprs-decode     │              └──────────┘              └───────────┘
└─────────────┘              └──────────────────┘
                                     │
                                     │ broadcast (DataItem)
                                     ▼
                               ┌──────────────┐    TCP     ┌──────────────┐
                               │  aprs_is.rs  │──────────>│  APRS-IS     │
                               │  + igate.rs  │           │  (noam.aprs2)│
                               │  (gate/dedup)│           └──────────────┘
                               └──────────────┘
```

> **No `packetd`.** Earlier designs consumed pre-decoded AX.25 frames from KA9Q-Radio's `packetd` daemon over a separate multicast group. rtpigate now performs RTP de-jitter, AFSK demodulation, HDLC framing, CRC validation and AX.25 parsing itself via the [`aprs-rtp`](https://crates.io/crates/aprs-rtp) crate, and APRS payload parsing via [`aprs-decode`](https://crates.io/crates/aprs-decode). The `[rtp]` section points at a channel's **audio** (PCM) multicast group.

## Module Breakdown

### `main.rs` — Orchestrator
- Resolves the config path: CLI arg `> ./config.toml > /etc/rtpigate/config.toml`, then loads it via `Config::from_file()`.
- Initializes logging (`flexi_logger`); level is `debug` when `[station] verbose = true`, otherwise `info`.
- Validates the config (`Config::validate()`) and exits with clear errors on failure. Also exits if igating/beaconing is enabled but the APRS-IS passcode is invalid.
- Wraps config in `Arc<Config>` for sharing across tasks.
- Creates two broadcast channels:
  - **DataItem channel** (capacity 128): carries `DataItem::Pkt(Packet)` and `DataItem::Tlm(AppTelemetry)` from producers to consumers.
  - **SSEEvent channel** (capacity 128): carries serialized JSON events to browser connections.
- Creates a 24-hour rolling **satellite packet log** (`Arc<RwLock<VecDeque<RTPPacket>>>`) shared between the RTP listener (writer) and the `/api/satellite-packets` handler (reader).
- Creates a 24-hour rolling **statistics history store** (`Arc<RwLock<HistoryStore>>`, `history.rs`) shared between `sse_task` (writer — merges the `packet_statistics` and `aprsis_statistics` telemetry into 15s buckets keyed by timestamp) and the `/api/history` handler (reader). This is in-memory only and is rebuilt live after a restart.
- Spawns async tasks into a `JoinSet`:
  1. `rtp_listener` (always)
  2. `aprsis_task` (only if `[aprsis] enabled = true`)
  3. `sse_task` (always)
- Starts the axum HTTP server (listen address from `[http] listen`, default `127.0.0.1:3000`).
- **Routes:**
  - `GET /api/sse` — SSE stream endpoint
  - `GET /api/config` — sanitized public config as JSON (no passcode)
  - `GET /api/satellite-packets` — 24h satellite packet log, newest-first
  - `GET /api/history` — 24h rolling per-interval packet/igating statistics (15s buckets), oldest-first; seeds the activity chart on load
  - `/assets/*` — static frontend assets (`ServeDir`)
  - fallback — `ServeDir` serving `index.html` and the frontend root
- **SIGHUP** reloads and re-validates the config, updates the shared `PublicConfig`, and pushes a `config` SSE event to connected browsers. (Changes to RTP/APRS-IS host/port still need a restart.)
- Graceful shutdown via `CancellationToken` on **SIGTERM/SIGINT**, then `JoinSet::join_all()`.

### `ka9q.rs` — RTP Audio Listener & Demodulator
Receives, demodulates, decodes, and classifies RF packets, and produces all RF-side telemetry.

- Builds an `aprs_rtp::config::SourceConfig` from `[rtp]` (`host`, `port`, `jitter_buffer = 2`) and a `DecoderConfig` (crate defaults: **8 slicers**, space-gain ladder **0.5 → 4.0**, single-bit CRC fix).
- Runs `aprs_rtp::AprsListener::new(source, decoder).run()`, which de-jitters the RTP audio, demodulates 1200-baud AFSK through the parallel slicer bank, frames HDLC, validates CRC and parses AX.25, then streams decoded packets over a channel. Reconnects with capped exponential backoff (5s → 300s) on setup failure or channel close.
- For each decoded packet, `map_packet()`:
  - Maps AX.25 framing (source, destination, via-path, heard-direct, per-hop repeated bits) into the internal `RTPPacket`.
  - Re-parses the APRS information field with `aprs-decode` (`decode_ax25`, falling back to `decode_textual`) to extract **position / Mic-E / object / item** coordinates, altitude (feet; Mic-E metres converted), and the **map symbol** (table + code).
  - Derives `digipeater_path`/`hops` (real callsigns only), `was_digipeated` (any repeated bit, including WIDE fill-ins), and `rfonly` (TCPIP/TCPXX/RFONLY/NOGATE in path).
- After mapping, sets `is_satellite` (frequency ∈ `[satellite] frequencies`) and `igated` (mirrors `igate::droppacket`, minus dedup) for display.
- Aggregates per-slicer decode counts for the waterfall from each packet's `slicer_mask` bitmask.
- Maintains rolling state, emitted on a 15-second tick:
  - **Packet statistics** (`packet_statistics`): total / heard-direct / digipeated / decode-errors series (100 points) plus lifetime counters.
  - **Slicer statistics** (`slicer_statistics`): slicer count, the space-gain ladder, the last 10 per-slicer interval snapshots, and lifetime per-slicer totals.
  - **Station statistics** (`station_statistics`): a per-callsign table (evicted after 36h) and a per-frequency table (pruned after 24h).
- Appends satellite-frequency packets to the shared 24h satellite log.

> The `decode_errors` counter is retained for frontend compatibility but stays 0: `aprs-rtp` only emits successfully decoded frames.

### `aprs_is.rs` — APRS-IS Connection, IGate Gating, Beaconing, Telemetry
Owns the persistent TCP connection and everything that writes to APRS-IS.

- Persistent TCP connection to `[aprsis] host:port`, with capped exponential backoff **5 → 10 → 20 → 40 → 80 → 160 → 300 → 300 …** across resolve/connect/login failures. Bounded `CONNECT_TIMEOUT`, `LOGIN_TIMEOUT`, and `WRITE_TIMEOUT` prevent a hung server from stalling the task.
- Login string: `user CALLSIGN pass PASSCODE vers 1.0\r\n`. Passcode validity is checked via the `APRSISPasscode` trait; an invalid/absent passcode logs in read-only (`-1`).
- Reads server lines; `#` lines are comments/keepalives. Incoming internet packets are **not** rebroadcast (this is a receive-only igate — there is no Internet→RF path and no `InetPacket`).
- **IGate gating** (when `igating` and a valid read/write passcode):
  - Calls `igate::droppacket()`; on `Some(reason)` increments the matching per-reason lifetime drop counter and skips the packet.
  - **Duplicate suppression** (lives here, not in `igate.rs`): a `HashMap` keyed `source:info` with a 30s TTL, purged on the telemetry tick.
  - Reforms the packet with `RTPPacket::for_rxigate()` using the **`qAO`** construct (`SRC>DST,path,qAO,IGATECALL:info`), re-checks for embedded CR/LF (defense in depth), and writes it.
- **Beaconing** (when `beaconing` and valid passcode): sends a position beacon built by `igate::positpacket()` every `[aprsis] threshold` seconds (default 600).
- **Telemetry**: on the same interval, emits an APRS telemetry report (T#/EQNS/PARM/UNIT/BITS) with five analog parameters, encoded via `APRSQuadratic` (see `igate.rs`):

  | PARM | Units | Source |
  |------|-------|--------|
  | `Rx_Nmin` | Pkts | packets received from RF this interval |
  | `RxSat_Nmin` | Pkts | packets on satellite frequencies |
  | `%Drop_Nmin` | % | percentage dropped by gating |
  | `%Direct_Nmin` | % | percentage heard direct |
  | `RxAltFreq_Nmin` | Pkts | packets not on 144.390 MHz |

  The telemetry sequence number persists to `/tmp/telem-seq.txt` across restarts (not reboots).
- Emits `aprsis_statistics` telemetry (rf-received / igated / dropped / reconnects series + lifetime counters, including per-reason drop and channel-lag drop breakdowns) on the 15s tick.

### `igate.rs` — Filtering Rules & Packet Construction
Pure functions and encoders; holds no connection state.

- `droppacket(&RTPPacket) -> Option<DropReason>` returns the reason a packet must **not** be gated:
  | `DropReason` | Condition |
  |--------------|-----------|
  | `MalformedField` | embedded (non-trailing) CR/LF in source/dest/path/info — prevents APRS-IS line injection |
  | `Stale` | packet age > 30s (uses a monotonic `Instant`, immune to NTP/wall-clock skew) |
  | `RfOnly` | `TCPIP`/`TCPXX`/`RFONLY`/`NOGATE` in path |
  | `GenericQuery` | data type `?` |
  | `ThirdPartyInternet` | `}` packet with `TCPIP`/`TCPXX` in the inner header |
  | `SatelliteDirect` | heard non-digipeated on a satellite frequency and not from a known satellite |

  Known satellites: `RS0ISS`, `NA1SS`, `DP0ISS`, `OR4ISS`, `IR0ISS`, `DP0SNX`. (Duplicate suppression is **not** here — it lives in `aprs_is.rs`.)
- `positpacket()` builds the beacon: `CALL>APZJD1,TCPIP*:/HHMMSSh{lat}{overlay}{lon}{symbol}/A={alt}{name}` with lat/lon in APRS degrees-decimal-minutes. `TOCALL = "APZJD1"` (`APZ` = experimental, `JD1` = version).
- `APRSQuadratic` / `AnalogItem` / `Telemetry` encode arbitrary values into APRS telemetry's quadratic `EQNS` form (`value = a·x² + b·x + c`), keeping raw counts recoverable on the receiving side.
- Telemetry sequence file helpers (`read_telemetry_file`, `write_telemetry_seq`).

### `sse.rs` — SSE Event Fan-out
- Subscribes to the DataItem broadcast channel.
- Serializes each item to JSON (`serde_json::json!`) and republishes it as an `SSEEvent { event, data }` on the SSE channel. Logs and continues if no subscribers are connected; warns on `RecvError::Lagged`.
- **Event types produced:**
  - `rfpacket` — a decoded RF packet (`DataItem::Pkt(Packet::RTP)`)
  - `packet_statistics` — RF packet telemetry
  - `aprsis_statistics` — APRS-IS / igate telemetry
  - `slicer_statistics` — slicer-diversity waterfall telemetry
  - `station_statistics` — last-heard stations + frequency tables
  - (`config` is emitted directly from `main.rs` on SIGHUP, not via `sse.rs`.)

### `config.rs` — Configuration & Telemetry Types
- Deserializes `config.toml` via the `toml` crate. Sections: `[station]`, `[location]`, `[aprsis]`, `[rtp]`, optional `[satellite]`, optional `[http]`.
- `Config::validate()` returns a list of human-readable errors (callsign, RTP host/port, lat/lon ranges, APRS-IS host/port when enabled, location completeness when beaconing, HTTP listen format).
- `Config::to_public()` produces `PublicConfig` (passcode and other secrets stripped) for `/api/config` and SSE.
- `Config::satellite_frequencies()` returns `[satellite] frequencies`, defaulting to `[145.825]`.
- Traits: `APRSISLogin` (login string) and `APRSISPasscode` (`compute_passcode` via the standard XOR hash + `passcode_isvalid`).
- Also defines the channel enums and all telemetry structs (below).

## Data Structures

### Channel Enums
```rust
enum DataItem {
    Pkt(Packet),
    Tlm(AppTelemetry),
}

enum Packet {
    RTP(RTPPacket),     // only variant — there is no Inet/Internet packet path
}

enum AppTelemetry {
    PacketStatus(PacketTelemetry),
    AprsisStatus(AprsisTelemetry),
    SlicerStatus(SlicerTelemetry),
    StationStatus(StationTelemetry),
}
```

### RTPPacket
```rust
pub struct RTPPacket {
    pub receivetime: DateTime<Local>,
    pub received_instant: Instant,   // monotonic; used for staleness (not serialized)
    pub raw: String,                 // full APRS text: source>dest,path:info
    pub info: String,                // APRS information field only
    pub path: String,                // comma-separated via path
    pub digipeater_path: Vec<String>,// real callsigns only (no WIDE/TCPIP/…)
    pub hops: u32,
    pub ptype: char,                 // APRS data type (first info byte)
    pub source: String,
    pub destination: String,
    pub heard_direct: bool,          // "ignore fill-in digis" semantics
    pub heardfrom: String,
    pub was_digipeated: bool,        // strict: any repeated bit set
    pub rfonly: bool,
    pub frequency: f64,              // MHz
    pub is_satellite: bool,          // set from [satellite] frequencies
    pub igated: bool,                // mirrors droppacket() for display
    pub object_name: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,
    pub slicer_mask: u16,            // bit i = slicer i decoded this frame (not serialized)
}
```

### Slicer (Waterfall) Telemetry
```rust
pub struct SlicerInterval {
    pub timestamp: DateTime<Local>,
    pub counts: Vec<u32>,            // length == slicer_count (one 15s row)
}

pub struct SlicerTelemetry {
    pub name: String,                // "slicer_statistics"
    pub timestamp: DateTime<Local>,
    pub microsecs: f64,
    pub slicer_count: usize,         // heatmap columns (decoder config)
    pub slicer_gains: Vec<f32>,      // per-slicer space-gain ladder
    pub intervals: VecDeque<SlicerInterval>,  // last 10, oldest-first
    pub lifetime_slicer_hits: Vec<u64>,
}
```

### Station Telemetry
```rust
pub struct StationEntry {
    pub callsign: String,
    pub transmitted_by: Option<String>,   // for objects/items
    pub last_heard: DateTime<Local>,
    pub frequency: f64,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,
    pub heard_direct: bool,
    pub position_path: Vec<String>,  pub position_hops: u32,
    pub altitude_path: Vec<String>,  pub altitude_hops: u32,
    pub symbol_table: Option<char>,  pub symbol_code: Option<char>,
    pub count: u64, pub count_direct: u64, pub count_digipeated: u64,
}

pub struct StationTelemetry {
    pub name: String,
    pub stations: Vec<StationEntry>,       // sorted by descending count
    pub frequencies: Vec<FrequencyCount>,  // sorted by descending count
}
```

### Rolling Series
```rust
pub struct DataSeries<T: Add> {
    pub name: String,
    pub data: VecDeque<DataPoint<T>>,  // rolling window, max 100 points
}

pub struct DataPoint<T: Add> {
    pub timestamp: DateTime<Local>,
    pub value: T,
}
```

`PacketTelemetry` and `AprsisTelemetry` bundle several `DataSeries<u32>` plus lifetime `u64` counters; `AprsisTelemetry` additionally breaks lifetime drops out per `DropReason` and tracks `lifetime_lagged_drops` (packets dropped by the broadcast channel before gating).

## The Slicer-Diversity Waterfall

The most distinctive piece of the dashboard, and the reason `ka9q.rs` carries `slicer_mask` end-to-end.

- The `aprs-rtp` demodulator runs a **bank of parallel slicers** (default 8). Each applies a different gain to the space tone before slicing: `demod_out = mark − space × gain`. Gains are spread geometrically across `[min_gain, max_gain]` (default `0.5 → 4.0`).
  - `gain < 1` favors **pre-emphasized** (loud-space) signals; `gain > 1` favors **de-emphasized** (loud-mark) signals; `gain ≈ 1` is flat.
- A frame may be CRC-recovered by several slicers at once; `slicer_mask` records which. `ka9q.rs` tallies each set bit per 15s window into `SlicerInterval.counts`, keeps the last 10 windows, and ships them as `slicer_statistics`.
- The frontend (`app.js`, `drawWaterfall`) renders columns = slicers (ordered by gain, headered with the mark:space ratio and grouped into pre-emph / flat / de-emph zones) and rows = 15s windows (newest on top). Cell brightness/number is that slicer's recovered-packet count, scaled to the busiest visible cell.
- `space_gains()` in `ka9q.rs` re-derives the same ladder the crate uses internally (the crate's copy is private) so the frontend column labels stay truthful — kept in sync with the `DecoderConfig` passed to the listener.

Interpretation: activity spread across many columns ⇒ strong/clean signals; a persistent lean toward the pre-emph or de-emph zones indicates audio twist worth correcting in the receive path.

## config.toml Structure

```toml
[station]
callsign = "N0CAL"
name = "My APRS iGate"   # optional; shown in UI/beacons
verbose = false          # optional; debug logging

[location]
lat = 44.9671            # decimal degrees
lon = -103.7714          # decimal degrees
alt = 3160               # feet

[aprsis]
passcode = "12345"       # must match computed passcode for write access; -1 = read-only
host = "noam.aprs2.net"
port = 14580
enabled = true           # master switch for APRS-IS connectivity
beaconing = false
igating = false
overlay = "R"            # optional; omit for primary-table icons
symbol = "\\&"           # table char + symbol char (backslash escaped)
threshold = 600          # beacon/telemetry interval, seconds

[rtp]
host = "packet.local"    # KA9Q-Radio AUDIO multicast group (not a packetd frame group)
port = 5004

[satellite]              # optional; defaults to [145.825]
frequencies = [145.825]

[http]                   # optional
listen = "127.0.0.1:3000"
frontend = "/usr/local/share/rtpigate/frontend"
```

## Frontend

Vanilla JavaScript (`frontend/assets/app.js`), served by axum (typically behind an Apache/nginx reverse proxy for TLS). Connects to `/api/sse` for real-time data and calls `/api/config` and `/api/satellite-packets` for snapshots.

**SSE events consumed:** `rfpacket`, `packet_statistics`, `aprsis_statistics`, `slicer_statistics`, `station_statistics`, `config`.

**Main views:** header status indicators (SSE / KA9Q / APRS-IS), live station-config panel, RF and APRS-IS sparkline groups, the slicer waterfall, the recent-packets table, and the last-heard stations table. Dark/light theme is persisted in the browser; the layout is mobile-responsive.

**APRS symbol resolution:** PNGs live under `frontend/assets/aprssymbols/`; the app maps a symbol table+code to a filename (`<OVERLAY>-<SYMBOL>.png` / `<SYMBOL>.png`, with `-flip` variants for some bearings).

## Key Dependencies

- **[`aprs-rtp`](https://crates.io/crates/aprs-rtp)** — RTP audio subscription, de-jitter, multi-slicer AFSK demodulation, HDLC/CRC, AX.25 parsing.
- **[`aprs-decode`](https://crates.io/crates/aprs-decode)** — APRS information-field parsing (position, Mic-E, object, item, altitude, symbol).
- `tokio` (async runtime, broadcast channels, signals), `axum` + `tower-http` (HTTP/SSE/static), `serde`/`serde_json` (telemetry), `chrono` (timestamps), `flexi_logger`/`log`, `toml`.

The previously vendored `aprs-parser-rs` fork has been removed; its role is covered by `aprs-rtp` + `aprs-decode`.

## Key Design Decisions

1. **In-process demodulation** — `aprs-rtp`/`aprs-decode` replace the external `packetd`; `[rtp]` points at the channel audio group.
2. **Receive-only igate** — no Internet→RF gating, `qAO` not `qAR`; there is no `InetPacket` type.
3. **Defense against line injection** — embedded CR/LF in any gated field is rejected (`MalformedField`) before and again at the write boundary.
4. **Monotonic staleness guard** — the 30s age cutoff uses an `Instant` so clock corrections can't spuriously age packets.
5. **Layered drop accounting** — every drop is attributed to a `DropReason` (plus separate dedup and channel-lag counters) so gating regressions surface in `aprsis_statistics`.
6. **Slicer diversity surfaced to the operator** — `slicer_mask` is carried end-to-end and visualized as a waterfall for receive-path tuning.
7. **Capped exponential backoff** — 5s→300s on both the RTP listener and the APRS-IS connection, responsive to brief dropouts and patient through long outages.
8. **Loose coupling via broadcast channels** — lagging subscribers drop messages instead of stalling producers.
9. **Localhost-only HTTP by default** — a reverse proxy handles TLS and public exposure.
10. **Telemetry sequence persistence** — `/tmp/telem-seq.txt` survives process restarts (not reboots).
```
