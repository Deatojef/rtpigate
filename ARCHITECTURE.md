# ARCHITECTURE.md

Reference document for building/reworking rtpigate — an APRS igate for KA9Q-Radio.

## System Overview

```
┌─────────────┐    RTP/UDP     ┌────────────┐   broadcast    ┌──────────┐   broadcast    ┌───────────┐
│  KA9Q-Radio │──(multicast)──>│  ka9q.rs   │──(DataItem)──>│  sse.rs  │──(SSEEvent)──>│  axum     │──> Browser
│  (SDR)      │                │            │               │          │               │  /api/sse │
└─────────────┘                └────────────┘               └──────────┘               └───────────┘
                                     │
                                     │ broadcast (DataItem)
                                     ▼
                               ┌────────────┐    TCP     ┌──────────────┐
                               │  igate.rs  │──────────>│  aprs_is.rs  │──> noam.aprs2.net
                               │  (filter)  │           │  (connection)│
                               └────────────┘           └──────────────┘
```

## Module Breakdown

### `main.rs` — Orchestrator
- Reads `config.toml` via `Config::from_file()`
- Wraps config in `Arc<Config>` for sharing across tasks
- Creates two broadcast channels:
  - **DataItem channel** (capacity 128): carries `DataItem::Pkt(Packet)` and `DataItem::Tlm(AppTelemetry)` between all tasks
  - **SSEEvent channel** (capacity 16): carries serialized JSON events to browser connections
- Spawns async tasks into a `JoinSet`:
  1. KA9Q listener (always)
  2. APRS-IS/IGate task (if `aprsis.enabled = true`)
  3. SSE task (always)
- Starts axum HTTP server on `127.0.0.1:3000` (proxied by Apache)
- Route: `GET /api/sse` — SSE stream endpoint
- Graceful shutdown via `CancellationToken` on SIGTERM/SIGINT, then `JoinSet::join_all()`

### `ka9q.rs` — KA9Q-Radio RTP Listener
Replaces the current `packet.rs`. Responsible for receiving and decoding RF packets.

- Resolves `config.rtp.host:config.rtp.port` to a multicast `SocketAddr`
- Binds a UDP socket via `socket2` (reuse addr, join multicast group on `Ipv4Addr::UNSPECIFIED`), then converts to `tokio::net::UdpSocket`
- Main loop (`tokio::select!`):
  - Reads UDP datagrams into 1500-byte buffer
  - Parses RTP header via `rtp-rs::RtpReader`
  - Extracts frequency from `rtp.ssrc() as f64 / 1000.0`
  - Parses AX.25 frame from RTP payload via `ax25::frame::Ax25Frame::from_bytes()`
  - Only processes `UnnumberedInformation` frames; others are errors
  - Builds `RTPPacket` struct (see Data Structures below)
  - Sends `DataItem::Pkt(Packet::RTP(packet))` on broadcast channel
  - Periodically (15s) sends `DataItem::Tlm(AppTelemetry::PacketStatus(...))` with rolling statistics (up to 100 data points per series)
- Cancellation-aware via token check in select

**Direct vs. Digipeated detection:**
```rust
let excluded_addrs = vec!["WIDE", "TCPIP", "NOGATE", "RFONLY", "SGATE"];
let heard_direct: bool = ax25_frame.route.iter()
    .filter(|p| p.has_repeated && excluded_addrs.iter().all(|x| !p.repeater.to_string().contains(x)))
    .count() == 0;
```

**Satellite detection:**
```rust
fn is_satellite(f: &f64) -> bool {
    let sat_freqs = vec![145.825];
    sat_freqs.contains(f)
}
```

**RF-only detection:**
```rust
let rfonly_addrs = vec!["TCPIP", "TCPXX", "RFONLY", "NOGATE"];
let rfonly: bool = ax25_frame.route.iter()
    .any(|p| rfonly_addrs.iter().any(|x| p.repeater.to_string().contains(x)));
```

### `aprs_is.rs` — APRS-IS TCP Connection Manager
Extracted from the connection-management portion of current `aprsis.rs`.

- Persistent TCP connection to `noam.aprs2.net:14580` (host/port from config)
- `TCP_NODELAY` when igating is enabled
- Login format: `user CALLSIGN pass PASSCODE vers rtpigate VERSION\r\n`
- Reads server version line, sends login, reads login response
- Reconnection with capped exponential backoff: **5 → 10 → 20 → 40 → 80 → 160 → 300 → 300 → ...**
  - Rationale: responsive to brief cellular dropouts (first 1-2 retries), patient for extended outages
- Reads APRS-IS server data (lines starting with `#` are comments/keepalives)
- Parses incoming internet packets into `InetPacket` and sends `DataItem::Pkt(Packet::Inet(...))` on broadcast channel
- Passcode validation via `APRSISPasscode` trait; `-1` = read-only

### `igate.rs` — IGate Filter & Transmission (new module)
Extracted from the igating logic in current `aprsis.rs`.

- **Receive-only igate**: RF → Internet only, never Internet → RF
- q-construct: `qAO` (not `qAR` which is bidirectional)
- Gated packet format: `source>dest,path,qAO,IGATECALL:info`

**Drop rules** (packet must NOT be gated if any apply):
1. Path contains `TCPIP`, `TCPXX`, `NOGATE`, or `RFONLY`
2. Generic query packet (data type `?`)
3. Third-party packet (`}`) with `TCPIP` or `TCPXX` in inner header
4. Heard directly on satellite frequency (145.825 MHz) unless source is a known satellite (`RS0ISS`, `DP0SNX`, `A55BTN`)
5. Packet age > 30 seconds (staleness guard — prevents stale injection after connection interruptions)
6. Passcode invalid (`-1`) — no write access

**Beaconing** (if `config.aprsis.beaconing = true` and valid passcode):
- Position beacon every `config.aprsis.threshold` seconds (default 600s / 10min)
- TOCALL: `APZJD1` (APZ = experimental)
- Format: `CALL>APZJD1,TCPIP*:/HHMMSSh{lat}{overlay}{lon}{symbol}/A={alt}{name}`
- Lat/lon converted to APRS degrees-decimal-minutes format

**Telemetry** (sent alongside beacons on same interval):
- Counters reset each interval:
  - `rf_received` — total packets from RF
  - `dropped` — packets failing igate criteria
  - `heard_direct` — packets heard without digipeaters
  - `received_sat` — packets from 145.825 MHz
  - `received_other` — packets from non-144.390 frequencies
- APRS telemetry format with quadratic equation coefficients (T#, EQNS, PARM, UNIT, BITS packets)
- Sequence number persisted to `/tmp/telem-seq.txt` across restarts
- Telemetry data also sent on DataItem channel as `AppTelemetry::AprsisStatus` for SSE

### `sse.rs` — SSE Event Handler
- Subscribes to DataItem broadcast channel
- Serializes packets and telemetry to JSON via `serde_json::json!()`
- Sends `SSEEvent { event, data }` on SSE broadcast channel
- Event types:
  - `rfpacket` — RF packet from KA9Q
  - `inetpacket` — packet from APRS-IS
  - `packet_statistics` — RTP listener telemetry
  - `aprsis_statistics` — APRS-IS/igate telemetry

### `config.rs` — Configuration
- Deserializes `config.toml` via `toml` crate
- Sections: `[station]`, `[location]`, `[aprsis]`, `[rtp]`
- Traits:
  - `APRSISLogin` — generates login string
  - `APRSISPasscode` — computes and validates APRS-IS passcode (XOR hash algorithm)
- All `[aprsis]` and `[station]` fields are `Option<T>` for flexibility

## Data Structures

### Channel Enums
```rust
enum DataItem {
    Pkt(Packet),
    Tlm(AppTelemetry),
}

enum Packet {
    RTP(RTPPacket),
    Inet(InetPacket),
}

enum AppTelemetry {
    PacketStatus(PacketTelemetry),
    AprsisStatus(AprsisTelemetry),
}
```

### RTPPacket
```rust
pub struct RTPPacket {
    pub receivetime: DateTime<Local>,
    pub raw: String,           // full APRS text: source>dest,path:info
    pub info: String,          // APRS information field only
    pub path: String,          // comma-separated via path
    pub ptype: char,           // first char of info field (APRS data type)
    pub source: String,
    pub destination: String,
    pub heard_direct: bool,
    pub heardfrom: String,     // source if direct, last digipeater if not
    pub rfonly: bool,
    pub frequency: f64,        // MHz from RTP SSRC
    pub is_satellite: bool,
}
```

### InetPacket
```rust
pub struct InetPacket {
    pub receivetime: DateTime<Local>,
    pub raw: String,
    pub info: String,
    pub ptype: char,
    pub source: String,
    pub aprsaddress: String,   // APRS-IS server address
}
```

### Telemetry Series
```rust
pub struct DataSeries<T> {
    pub name: String,
    pub data: Vec<DataPoint<T>>,  // rolling window, max 100 points
}

pub struct DataPoint<T> {
    pub timestamp: DateTime<Local>,
    pub value: T,
}
```

## config.toml Structure

```toml
[station]
callsign = "N0CAL"
name = "my igate"

[location]
lat = 44.9671          # decimal degrees
lon = -103.7714        # decimal degrees
alt = 3160             # feet

[aprsis]
passcode = "12345"     # must match computed passcode for write access
host = "noam.aprs2.net"
port = 14580
enabled = true         # false disables all APRS-IS connectivity
beaconing = true
igating = true
overlay = "R"          # optional; omit for primary table icons
symbol = "\\&"         # table char + symbol char (backslash must be escaped)
threshold = 600        # beacon/telemetry interval in seconds

[rtp]
host = "ax25.local"    # KA9Q-Radio multicast hostname
port = 5004
```

## Frontend

Vanilla JavaScript, served behind Apache reverse proxy. Connects to `/api/sse` for real-time data.

**SSE event consumption:** listens for `rfpacket`, `inetpacket`, `packet_statistics`, `aprsis_statistics` events.

**Display requirements:**
- Dark theme: background `#606060`, headers `#303030`, lighter portions `#c8c8c8`, white font
- Status indicators for KA9Q-Radio stream and APRS-IS connection
- Packet statistics from telemetry data
- Last 20 packets in reverse chronological order, table without borders:
  - Time (24H:MM:SS local)
  - APRS symbol icon (from `frontend/assets/aprssymbols/`, scaled to text height)
  - Packet text (monospace)
  - Lat/Lon (e.g. `39.123456, -104.123456`)
  - Altitude in feet (or `--`)
  - Distance in miles from station (position packets only)
- Table headers: smallcaps, 1.2em

**APRS symbol resolution:**
- `symbols-map.js` maps symbol codes to PNG filenames
- Filename patterns: `<OVERLAY>-<SYMBOL>.png`, `<SYMBOL>.png`
- Flip variants for bearings > 180°: `<SYMBOL>-flip.png`, `<OVERLAY>-<SYMBOL>-flip.png`

## Vendored Dependency

`vendor/aprs-parser-rs/` — local fork of aprs-parser-rs v0.4.2. Currently referenced as a path dependency in `Cargo.toml` (to be added). Provides APRS packet string parsing: positions, callsigns, timestamps, symbols, compressed/uncompressed formats.

## Key Design Decisions

1. **Receive-only igate** — no Internet→RF gating, uses `qAO` not `qAR`
2. **Staleness guard** — 30-second cutoff prevents stale packet injection after reconnection
3. **Satellite filtering** — 145.825 MHz packets not igated (except from known satellite callsigns)
4. **Capped exponential backoff** — optimized for mobile/cellular with brief and extended outage scenarios
5. **Broadcast channels** — loose coupling between tasks; lagging subscribers silently drop messages
6. **Localhost-only HTTP** — Apache reverse proxy handles TLS and public access
7. **Telemetry sequence persistence** — `/tmp/telem-seq.txt` survives process restarts (not reboots)
