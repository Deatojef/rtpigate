# ARCHITECTURE.md

Reference document for the internals of rtpigate — a receive-only APRS iGate that consumes the decoded-APRS multicast stream published by the [`aprs-streamd`](https://github.com/deatojef/aprs-stream) base service. Describes the implementation as it currently stands.

## System Overview

rtpigate subscribes to the **decoded-APRS multicast stream** published by `aprs-streamd` — fully-decoded, typed [`AprsFrame`](https://github.com/deatojef/aprs-stream) messages, one per UDP datagram — maps each frame to its internal packet type, then filters and gates qualifying packets to APRS-IS. It no longer touches RTP audio, AFSK demodulation, or AX.25 parsing: that decode work happens once, upstream in the producer, and is shared. A web dashboard receives real-time updates over Server-Sent Events.

```
┌─────────────┐  AprsFrame   ┌──────────────────┐  broadcast   ┌──────────┐  broadcast   ┌───────────┐
│ aprs-streamd│──(CBOR/UDP ─>│    stream.rs     │──(DataItem)─>│  sse.rs  │──(SSEEvent)─>│  axum     │──> Browser
│  (producer) │  multicast)  │  aprs-stream     │              │          │              │  /api/sse │
│             │              │  ::Subscriber    │              └──────────┘              └───────────┘
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

> **Disaggregated decode.** Earlier designs owned the whole RF chain — RTP de-jitter, AFSK demodulation, HDLC framing, CRC validation, and AX.25 parsing — in-process via the [`aprs-rtp`](https://crates.io/crates/aprs-rtp) crate. That now lives in the [`aprs-streamd`](https://github.com/deatojef/aprs-stream) base service, which decodes once and publishes typed frames on a UDP multicast group. rtpigate consumes them via the shared [`aprs-stream`](https://github.com/deatojef/aprs-stream) crate; the `[stream]` section points at that group. APRS payload types still come from [`aprs-decode`](https://crates.io/crates/aprs-decode), now embedded in each frame.

## Module Breakdown

### `main.rs` — Orchestrator
- Resolves the config path: CLI arg `> ./config.toml > /etc/rtpigate/config.toml`, then loads it via `Config::from_file()`.
- Initializes logging (`flexi_logger`); level is `debug` when `[station] verbose = true`, otherwise `info`.
- Validates the config (`Config::validate()`) and exits with clear errors on failure. Also exits if igating/beaconing is enabled but the APRS-IS passcode is invalid.
- Wraps config in `Arc<Config>` for sharing across tasks.
- Creates two broadcast channels:
  - **DataItem channel** (capacity 128): carries `DataItem::Pkt(Packet)` and `DataItem::Tlm(AppTelemetry)` from producers to consumers.
  - **SSEEvent channel** (capacity 128): carries serialized JSON events to browser connections.
- Creates a 24-hour rolling **satellite packet log** (`Arc<RwLock<VecDeque<RTPPacket>>>`) shared between the stream listener (writer) and the `/api/satellite-packets` handler (reader).
- Creates a 24-hour rolling **statistics history store** (`Arc<RwLock<HistoryStore>>`, `history.rs`) shared between `sse_task` (writer — merges the `packet_statistics` and `aprsis_statistics` telemetry into 15s buckets keyed by timestamp) and the `/api/history` handler (reader). This is in-memory only and is rebuilt live after a restart.
- Spawns async tasks into a `JoinSet`:
  1. `rtp_listener` (always)
  2. `aprsis_task` (only if `[aprsis] enabled = true`)
  3. `gpsd_task` (only if `[location] source = "gpsd"`)
  4. `sse_task` (always)
- Creates a shared latest-GPS-fix slot (`Arc<RwLock<Option<GpsFix>>>`) written by `gpsd_task` and read by `aprsis_task` for movement-based beaconing. Stays `None` when GPSD is not the position source.
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

### `stream.rs` — Decoded-APRS Stream Subscriber
Receives typed frames from the multicast stream, maps them to the internal packet type, and produces all RF-side telemetry. It does **not** demodulate or parse AX.25 — that happened once, upstream in the producer.

- Builds an `aprs_stream::subscribe::SubscribeConfig` from `[stream]` (`group`, `port`, optional `interface` and `recv_buffer_bytes`) and joins the multicast group via `aprs_stream::Subscriber` (`socket2`: `SO_REUSEADDR`, interface-selected join, `SO_RCVBUF`). Reconnects with capped exponential backoff (5s → 300s) on socket setup failure or a socket-level receive error; a malformed/version-incompatible datagram is logged and skipped without tearing down.
- For each received `AprsFrame`, `map_frame()`:
  - Reads the AX.25 framing facts (source, destination, via-path with per-hop repeated bits, heard-direct, heard-from, DTI) straight from the frame's `ax25_meta` block — **no AX.25 re-parsing**. A frame lacking `ax25_meta` (a pre-v2 producer) is skipped.
  - Takes the verbatim 8-bit info field as `ax25[ax25_meta.info_offset..]` (byte-faithful, for igating) and derives the lossy-UTF-8 `info` for display.
  - Reads **position / Mic-E / object / item** coordinates, altitude (feet; Mic-E metres converted), and the **map symbol** from the frame's already-parsed payload (`frame.parsed`), falling back to `decode_textual` on the reconstructed TNC2 text only if it wasn't typed.
  - Reconstructs the TNC2 `raw` string (with heard `*` markers) via `aprs-decode`'s `encode_textual`.
  - Derives `digipeater_path`/`hops` (real callsigns only), `was_digipeated` (any repeated bit, including WIDE fill-ins), and `rfonly` (TCPIP/TCPXX/RFONLY/NOGATE in path).
- After mapping, sets `is_satellite` (frequency ∈ `[satellite] frequencies`) and `igated` (mirrors `igate::droppacket`, minus dedup) for display.
- Learns the slicer bank size and per-slicer gain ladder from the first frame carrying `RfMeta::slicer_gains` (the producer publishes it per-frame), then aggregates per-slicer decode counts for the waterfall from each frame's `slicer_mask`.
- Maintains rolling state, emitted on a 15-second tick:
  - **Packet statistics** (`packet_statistics`): total / heard-direct / digipeated / decode-errors series (100 points) plus lifetime counters.
  - **Slicer statistics** (`slicer_statistics`): slicer count, the gain ladder (from the wire), the last 10 per-slicer interval snapshots, and lifetime per-slicer totals.
  - **Station statistics** (`station_statistics`): a per-callsign table (evicted after 36h) and a per-frequency table (pruned after 24h).
- Appends satellite-frequency packets to the shared 24h satellite log.

> The `decode_errors` counter is retained for frontend compatibility but stays 0: the producer only publishes successfully-decoded frames.

### `aprs_is.rs` — APRS-IS Connection, IGate Gating, Beaconing, Telemetry
Owns the persistent TCP connection and everything that writes to APRS-IS.

- Persistent TCP connection to `[aprsis] host:port`, with capped exponential backoff **5 → 10 → 20 → 40 → 80 → 160 → 300 → 300 …** across resolve/connect/login failures. Bounded `CONNECT_TIMEOUT`, `LOGIN_TIMEOUT`, and `WRITE_TIMEOUT` prevent a hung server from stalling the task.
- Login string: `user CALLSIGN pass PASSCODE vers 1.0\r\n`. Passcode validity is checked via the `APRSISPasscode` trait; an invalid/absent passcode logs in read-only (`-1`).
- Reads server lines; `#` lines are comments/keepalives. Incoming internet packets are **not** rebroadcast (this is a receive-only igate — there is no Internet→RF path and no `InetPacket`).
- **IGate gating** (when `igating` and a valid read/write passcode):
  - Calls `igate::droppacket()`; on `Some(reason)` increments the matching per-reason lifetime drop counter and skips the packet.
  - **Duplicate suppression** (lives here, not in `igate.rs`): a `HashMap` keyed `source:info` with a 30s TTL, purged on the telemetry tick.
  - Reforms the packet with `RTPPacket::for_rxigate()` using the **`qAO`** construct (`SRC>DST,path,qAO,IGATECALL:info`), re-checks for embedded CR/LF (defense in depth), and writes it.
- **Beaconing** (when `beaconing` and valid passcode): sends a position beacon built by `igate::positpacket()`. Position source depends on `[location] source`:
  - **`config`**: beacons the static `[location]` lat/lon/alt every `[aprsis] threshold` seconds.
  - **`gpsd`**: a second timer (`min_beacon_secs`, default 30) beacons the live fix when it has moved more than `move_threshold_deg` since the last beacon — never faster than `min_beacon_secs`. The `[aprsis] threshold` tick remains a floor that also beacons (and, unlike the movement timer, carries telemetry). A beacon is skipped when there is no fresh fix (`GpsFix::is_fresh()`: a valid 2D/3D solution within `GPS_FRESHNESS` = 30s). The fix is read from the shared `Arc<RwLock<Option<GpsFix>>>` written by `gpsd_task`.

  | Transmission | Trigger | Cap / interval |
  |---|---|---|
  | Position (moving, gpsd) | moved > `move_threshold_deg` | no faster than `min_beacon_secs` |
  | Position (floor) | `threshold` tick | every `[aprsis] threshold` |
  | Telemetry | `threshold` tick only | every `[aprsis] threshold` |

- **Telemetry**: on the `threshold` tick **only** (never on a movement beacon), emits an APRS telemetry report (T#/EQNS/PARM/UNIT/BITS) with five analog parameters, encoded via `APRSQuadratic` (see `igate.rs`):

  | PARM | Units | Source |
  |------|-------|--------|
  | `Rx_Nmin` | Pkts | packets received from RF this interval |
  | `RxSat_Nmin` | Pkts | packets on satellite frequencies |
  | `%Drop_Nmin` | % | percentage dropped by gating |
  | `%Direct_Nmin` | % | percentage heard direct |
  | `RxAltFreq_Nmin` | Pkts | packets not on 144.390 MHz |

  The telemetry sequence number persists to `/tmp/telem-seq.txt` across restarts (not reboots).
- Emits `aprsis_statistics` telemetry (rf-received / igated / dropped / reconnects series + lifetime counters, including per-reason drop and channel-lag drop breakdowns) on the 15s tick.

### `gpsd.rs` — GPSD Position Source (optional)
Spawned only when `[location] source = "gpsd"`. Provides the live position for mobile/portable beaconing.

- Persistent TCP connection to `[gpsd] host:port` (default `localhost:2947`) with the same capped exponential backoff and bounded timeouts as `aprs_is.rs`. Sends gpsd's `?WATCH={"enable":true,"json":true}` on connect.
- Reads gpsd's newline-delimited JSON via a tokio `BufReader`, parsing each line into `gpsd_proto::UnifiedResponse`. `TPV` updates position/altitude (MSL metres → feet)/mode; `SKY` updates HDOP and the used/visible satellite counts (from the `satellites` array, or scalar `uSat`/`nSat` when gpsd emits a summary SKY).
- Merges `TPV`/`SKY` into a rolling `GpsFix` and writes it to the shared `Arc<RwLock<Option<GpsFix>>>`. `received_at` is a monotonic `Instant` (set on each `TPV`) used by `GpsFix::is_fresh()` for the 30s staleness guard, independent of wall-clock skew.
- Broadcasts a `gps_status` `AppTelemetry` for the dashboard on every update, and logs one INFO line when the fix **state** changes (no fix ↔ 2D ↔ 3D) rather than on every report.

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
- `positpacket()` builds the beacon: `CALL>APZJD1,TCPIP*:/HHMMSSh{lat}{overlay}{lon}{symbol}/A={alt}{name}{dao}` with lat/lon in APRS degrees-decimal-minutes. The base position is truncated to hundredths of a minute (~18.5 m); the sub-hundredth remainder is encoded into an optional APRS 1.2 `!DAO!` token per `[aprsis] dao` (`DaoMode`): `Human` → `!Wxy!` (one extra digit, ~1.85 m), `Base91` → `!wxy!` (base-91 char, ~0.2 m, via `base91_dao_char()`), `Off` → no token. `TOCALL = "APZJD1"` (`APZ` = experimental, `JD1` = version).
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
  - `gps_status` — live GPS fix/health (only when `[location] source = "gpsd"`)
  - (`config` is emitted directly from `main.rs` on SIGHUP, not via `sse.rs`.)

### `config.rs` — Configuration & Telemetry Types
- Deserializes `config.toml` via the `toml` crate. Sections: `[station]`, `[location]` (incl. `source`), `[aprsis]`, `[stream]`, optional `[satellite]`, optional `[http]`, optional `[gpsd]`.
- `Config::validate()` returns a list of human-readable errors (callsign, `[stream]` group being multicast and port > 0, lat/lon ranges, APRS-IS host/port when enabled, location completeness when beaconing with `source = "config"`, positive `[gpsd]` thresholds, HTTP listen format).
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
    GpsStatus(GpsTelemetry),        // only when [location] source = "gpsd"
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

The most distinctive piece of the dashboard, and the reason `slicer_mask` and `slicer_gains` are carried on every frame from the producer through to the browser.

- The producer's demodulator (in `aprs-streamd`, via `aprs-rtp`) runs a **bank of parallel slicers** (default 8). Each applies a different gain to the space tone before slicing: `demod_out = mark − space × gain`. The ladder is parameterized in **twist dB**: `slicers` rungs spread evenly across `[min_twist_db, max_twist_db]` (e.g. `−12 → +12 dB`), with each rung's linear gain `= 10^(db/20)`. Uniform-in-dB is identical to geometric-in-linear-gain.
  - Negative twist (`gain < 1`) favors **pre-emphasized** (loud-space) signals; positive (`gain > 1`) favors **de-emphasized** (loud-mark) signals; `0 dB` (`gain ≈ 1`) is flat.
- A frame may be CRC-recovered by several slicers at once; the frame's `RfMeta::slicer_mask` records which. `stream.rs` tallies each set bit per 15s window into `SlicerInterval.counts`, keeps the last 10 windows, and ships them as `slicer_statistics`.
- The frontend (`app.js`, `drawWaterfall`) renders columns = slicers (ordered by gain, headered with the slicer's twist in dB and grouped into pre-emph / flat / de-emph zones) and rows = 15s windows (newest on top). Cell brightness/number is that slicer's recovered-packet count, scaled to the busiest visible cell.
- The gain ladder itself arrives on the wire in `RfMeta::slicer_gains` (the producer publishes it per-frame, since a stateless multicast consumer can join at any time). `stream.rs` learns it from the first frame that carries it and derives the twist-dB column labels and pre-emph/flat/de-emph zones from it — no local decoder config, so the labels always match what the producer actually ran.

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

[stream]
group = "239.12.34.56"   # decoded-APRS multicast group published by aprs-streamd
port = 17014             # must match the producer's emit destination
# interface = "192.168.1.20"    # optional; local NIC to join on (multi-homed)
# recv_buffer_bytes = 4194304   # optional; enlarge SO_RCVBUF for bursts

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

- **[`aprs-stream`](https://github.com/deatojef/aprs-stream)** — shared schema (`AprsFrame`), CBOR codec, and UDP multicast transport. rtpigate uses its `Subscriber` to receive typed frames; the crate is the single source of truth for the wire format, shared with the `aprs-streamd` producer.
- **[`aprs-decode`](https://crates.io/crates/aprs-decode)** — the parsed APRS payload types (position, Mic-E, object, item, altitude, symbol) embedded in each frame and read directly by `stream.rs`.
- `tokio` (async runtime, broadcast channels, signals), `axum` + `tower-http` (HTTP/SSE/static), `serde`/`serde_json` (telemetry), `socket2` (multicast join, via `aprs-stream`), `chrono` (timestamps), `flexi_logger`/`log`, `toml`.

The RF-side crate (`aprs-rtp` — RTP audio + AFSK demodulation) is no longer a dependency here; it lives in the `aprs-streamd` producer.

## Key Design Decisions

1. **Disaggregated decode** — RTP audio, demodulation, and AX.25/APRS parsing happen once in the `aprs-streamd` producer; rtpigate consumes typed frames off a multicast group via `aprs-stream`. `[stream]` points at that group. AX.25 framing facts ride in each frame's `ax25_meta`, so nothing is re-parsed here.
2. **Receive-only igate** — no Internet→RF gating, `qAO` not `qAR`; there is no `InetPacket` type.
3. **Defense against line injection** — embedded CR/LF in any gated field is rejected (`MalformedField`) before and again at the write boundary.
4. **Monotonic staleness guard** — the 30s age cutoff uses an `Instant` so clock corrections can't spuriously age packets.
5. **Layered drop accounting** — every drop is attributed to a `DropReason` (plus separate dedup and channel-lag counters) so gating regressions surface in `aprsis_statistics`.
6. **Slicer diversity surfaced to the operator** — the producer's per-frame `slicer_mask` and `slicer_gains` are carried end-to-end and visualized as a waterfall for receive-path tuning.
7. **Capped exponential backoff** — 5s→300s on both the stream subscriber and the APRS-IS connection, responsive to brief dropouts and patient through long outages.
8. **Loose coupling via broadcast channels** — lagging subscribers drop messages instead of stalling producers.
9. **Localhost-only HTTP by default** — a reverse proxy handles TLS and public exposure.
10. **Telemetry sequence persistence** — `/tmp/telem-seq.txt` survives process restarts (not reboots).
```
