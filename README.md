# rtpigate

An APRS receive-only iGate for [KA9Q-Radio](http://www.ka9q.net/radio/) backend, written in Rust. It subscribes to a KA9Q-Radio channel's **RTP audio multicast group**, demodulates the 1200-baud AFSK in-process, decodes the AX.25/APRS frames, filters them per APRS-IS igating rules, and forwards qualifying packets to the APRS-IS internet network. Includes a real-time web dashboard for monitoring station activity, including a slicer-diversity waterfall for visualising demodulator health.

> **No `packetd` required.** Earlier versions consumed pre-decoded AX.25 frames from KA9Q-Radio's `packetd` daemon. rtpigate now performs RTP de-jitter, AFSK demodulation, HDLC framing, CRC validation, and AX.25/APRS parsing itself via the [`aprs-rtp`](https://crates.io/crates/aprs-rtp) and [`aprs-decode`](https://crates.io/crates/aprs-decode) crates. Point the `[rtp]` section at a channel's **audio** (PCM) multicast group — not an AX.25 frame group — and you no longer need to run `packetd` at all.

## Features

- **Receive-only iGate** (RF to Internet) using `qAO` construct
- **In-process AFSK demodulation** — RTP de-jitter, 1200-baud demod, HDLC/CRC, and AX.25/APRS parsing all in Rust (no external `packetd`)
- **Multi-slicer demodulator** — a bank of parallel amplitude-imbalance slicers (default 8) maximises decode rate across twisted/imbalanced audio
- **Multi-frequency support** via KA9Q-Radio's RTP audio multicast stream
- **Position beaconing** and **APRS telemetry** to APRS-IS
- **GPSD position source** for mobile/portable operation — live position with movement-triggered, rate-limited beaconing and `!DAO!` precision (see [GPSD Position Source & Beacon Cadence](#gpsd-position-source--beacon-cadence))
- **Real-time web dashboard** with Server-Sent Events (SSE)
  - Live packet table with APRS symbol icons, coordinates, and distance
  - Last-heard stations table sorted by packet count
  - Sparkline charts for RF and APRS-IS statistics
  - **Slicer-diversity waterfall** heatmap for visualising demodulator health and audio twist
  - Callsign links to aprs.fi, coordinate links to Google/Apple Maps
  - Dark/light theme toggle
  - Mobile-responsive layout
- **Resilient networking** with capped exponential backoff on all connections
- **Duplicate packet suppression** (30-second TTL dedup cache)
- **SIGHUP config reload** with instant SSE push to connected browsers
- **Systemd integration** with security hardening and dedicated service user
- **Debian packaging** via `cargo-deb` for easy deployment

## Architecture

Up to five concurrent async tasks orchestrated with tokio (the GPSD task runs only when `[location] source = "gpsd"`):

```
                                                                     +-----------+
+-----------+  RTP audio   +----------+  broadcast  +--------+  SSE  |   axum    |
| KA9Q-Radio| (PCM/UDP     | ka9q.rs  | (DataItem)  | sse.rs | ----> | /api/sse  | --> Browser
|   (SDR)   |  multicast)  | aprs-rtp | ----------> |        |       +-----------+
|           | -----------> | aprs-dec |             +--------+
+-----------+              +----------+
                                |
                                | broadcast (DataItem)
                                v
                           +----------+   TCP    +-----------+
                           | igate.rs | -------> | aprs_is.rs| --> APRS-IS
                           | (filter) |          | (connect) |
                           +----------+          +-----------+
```

- **ka9q.rs** -- Subscribes to the KA9Q-Radio **audio** RTP multicast group via the `aprs-rtp` crate, which de-jitters the stream, demodulates the 1200-baud AFSK through a bank of parallel slicers, frames HDLC, validates the CRC, and parses AX.25. Each decoded frame's APRS payload (position, Mic-E, object, item, altitude, symbol) is then parsed with the `aprs-decode` crate and mapped to the internal packet type. Also aggregates per-slicer decode counts for the waterfall and reconnects with backoff on failure.
- **aprs_is.rs** -- Maintains persistent TCP connection to APRS-IS with capped exponential backoff (5s to 300s). Handles login verification, beaconing, and telemetry.
- **gpsd.rs** -- Optional GPSD client (spawned only when `[location] source = "gpsd"`). Connects to gpsd over TCP with the same backoff strategy, parses live `TPV`/`SKY` reports, and shares the latest fix with `aprs_is.rs` for movement-based beaconing. Also surfaces GPS health to the dashboard via a `gps_status` SSE event.
- **igate.rs** -- Applies igating filter rules (rfonly, staleness, satellite, query, third-party, duplicates).
- **sse.rs** -- Serializes packets and telemetry to JSON, pushes to SSE broadcast channel.
- **axum HTTP server** -- Serves the web dashboard and SSE endpoint. Sits behind a reverse proxy.

Graceful shutdown via `CancellationToken` on SIGTERM/SIGINT with clean TCP FIN to APRS-IS.

## Requirements

- **Rust** 2024 edition (1.85+)
- **KA9Q-Radio** running with a channel configured for **APRS** (1200-baud, ~12 kHz sample rate FM/PCM audio) and producing an RTP **audio** multicast group. `packetd` is **not** required — rtpigate demodulates the audio itself.
- A valid **APRS-IS passcode** for your callsign (if igating/beaconing)
- **Apache** or **nginx** as a reverse proxy (recommended for production)

## Quick Start (Development)

```bash
# Clone the repository
git clone https://github.com/Deatojef/rtpigate.git
cd rtpigate

# Edit the config
cp config.toml config.toml.bak
nano config.toml    # Set your callsign, passcode, location, RTP host

# Build and run
cargo build
cargo run

# Run tests
cargo test

# Open the dashboard
# http://127.0.0.1:3000
```

## Installation (Production)

### Option 1: Debian Package

Download the `.deb` from the [Releases](https://github.com/Deatojef/rtpigate/releases) page:

```bash
wget https://github.com/Deatojef/rtpigate/releases/download/vX.Y.Z/rtpigate_X.Y.Z-1_arm64.deb
sudo dpkg -i rtpigate_X.Y.Z-1_arm64.deb
```

### Option 2: Install Script

```bash
sudo ./deploy/install.sh
```

### Option 3: Manual

```bash
cargo build --release
sudo install -m 755 target/release/rtpigate /usr/local/bin/
sudo mkdir -p /etc/rtpigate
sudo cp config.toml /etc/rtpigate/
sudo cp -r frontend /usr/local/share/rtpigate/
sudo cp deploy/rtpigate.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### Post-Install

```bash
# Edit configuration (required)
sudo nano /etc/rtpigate/config.toml

# Enable and start the service
sudo systemctl enable --now rtpigate

# View logs
journalctl -u rtpigate -f

# Reload config without restart (SIGHUP)
sudo systemctl reload rtpigate
```

The service runs as a dedicated `rtpigate` system user with no login shell, no home directory, and restricted filesystem access.

## Configuration

The config file is searched in order:

1. CLI argument: `rtpigate /path/to/config.toml`
2. `./config.toml` (current directory)
3. `/etc/rtpigate/config.toml`

### config.toml Reference

```toml
[station]
callsign = "N0CALL"           # Required. Your amateur radio callsign.
name = "My APRS iGate"        # Optional. Displayed in web UI header and beacons.
verbose = false                # Optional. Enable debug-level logging.

[location]
source = "config"              # "config" (default) beacons the fixed lat/lon/alt below;
                               # "gpsd" beacons a live fix from gpsd (see [gpsd]).
lat = 30.123456                # Decimal degrees, -90 to 90. RF-antenna location.
                               # Required for beaconing when source = "config".
lon = -99.123456              # Decimal degrees, -180 to 180.
alt = 1234                     # Altitude in feet.

[aprsis]
host = "noam.aprs2.net"       # APRS-IS server hostname. Required when enabled.
port = 14580                   # APRS-IS server port. Required when enabled.
passcode = "12345"             # APRS-IS passcode for your callsign. -1 = read-only.
enabled = true                 # Master switch for APRS-IS connectivity.
igating = true                 # Gate RF packets to APRS-IS. Requires valid passcode.
beaconing = true               # Send position beacons. Requires valid passcode + location.
symbol = "\\&"                 # APRS symbol: table char + code char (backslash escaped).
overlay = "R"                  # Symbol overlay character. Omit for primary table symbols.
threshold = 600                # Beacon/telemetry interval in seconds (default: 600 = 10min).
dao = "human"                  # !DAO! beacon precision: "human" (~1.85m, default),
                               # "base91" (~0.2m), or "off". See DAO precision below.

[rtp]
host = "packet.local"          # KA9Q-Radio AUDIO multicast hostname or IP. Required.
                               # Point this at the channel's PCM audio group,
                               # NOT an AX.25/packetd frame group.
port = 5004                    # KA9Q-Radio multicast UDP port. Required.

[gpsd]                         # Optional section. Only used when [location] source = "gpsd".
host = "localhost"             # gpsd hostname or IP (default: localhost).
port = 2947                    # gpsd TCP port (default: 2947).
move_threshold_deg = 0.0001    # Beacon when lat or lon moves more than this many
                               # degrees since the last beacon (default: 0.0001 deg).
min_beacon_secs = 30           # Minimum seconds between movement-triggered beacons
                               # (default: 30). Caps position-beacon frequency.

[satellite]                    # Optional section. Defaults to [145.825] if omitted.
frequencies = [145.825]        # Frequencies (MHz) treated as satellite downlinks.
                               # Packets on these are gated only when digipeated
                               # or from a known satellite (see Igating Rules).

[http]                         # Optional section.
listen = "127.0.0.1:3000"     # HTTP listen address (default: 127.0.0.1:3000).
frontend = "/usr/local/share/rtpigate/frontend"  # Path to frontend assets directory.
```

### Validation

At startup, the application validates the config and exits with clear error messages if:

- Callsign is missing or empty
- RTP host is empty or port is 0
- Latitude or longitude is out of range
- APRS-IS is enabled but host or port is missing
- Beaconing is enabled with `source = "config"` but location (lat, lon, alt) is incomplete
  (with `source = "gpsd"` the live fix supplies the position, so static lat/lon/alt are optional)
- `[gpsd] move_threshold_deg` or `min_beacon_secs` is not greater than zero
- Passcode is invalid when igating or beaconing is enabled

### APRS-IS Passcode

The passcode is computed from your callsign using the standard APRS-IS XOR hash algorithm. You can find online calculators by searching "APRS-IS passcode generator." If the passcode in the config doesn't match the computed value for the callsign, the application:

- **If igating or beaconing is enabled**: exits with an error
- **Otherwise**: connects in read-only mode (sends `-1` as passcode)

### APRS Symbol

The `symbol` field is a two-character string: the symbol table character followed by the symbol code character. For the primary table, use `/` as the first character (e.g., `"/>"` for a car). For the alternate table, use `\\` (escaped backslash in TOML).

The optional `overlay` field places a single character (0-9, A-Z) over alternate table symbols.

Common examples:

| Symbol | Overlay | Description |
|--------|---------|-------------|
| `"/>"` | -- | Car |
| `"/-"` | -- | House QTH |
| `"/#"` | -- | Digipeater |
| `"\\&"` | `"R"` | R-overlay gateway |
| `"\\&"` | `"I"` | I-overlay gateway |

## GPSD Position Source & Beacon Cadence

By default (`[location] source = "config"`) rtpigate beacons the fixed `lat`/`lon`/`alt`
from the `[location]` section — the **RF-antenna location**. A fixed iGate that happens
to have a GPS attached should keep `source = "config"`; gpsd is then never consulted for
beaconing.

Set `source = "gpsd"` for mobile/portable use. rtpigate connects to gpsd (per the `[gpsd]`
section), tracks the live fix, and beacons it. The `[location]` values are then only a
fallback; a beacon is **skipped** whenever there is no fresh fix (a valid 2D/3D solution
seen within the last 30 seconds), so a stale position is never transmitted.

### When each transmission to APRS-IS occurs

Position beacons and telemetry are decoupled. Position can transmit frequently as you move
(but is rate-limited); telemetry only ever transmits on the fixed `threshold` interval.

| Transmission | Trigger | Cap / interval | Config element |
|---|---|---|---|
| **Position beacon** (moving) | position moved more than `move_threshold_deg` since the last beacon | no faster than `min_beacon_secs` (default 30s) | `[gpsd] move_threshold_deg`, `[gpsd] min_beacon_secs` |
| **Position beacon** (floor) | the fixed interval tick | every `threshold` (default 600s) | `[aprsis] threshold` |
| **Telemetry** | the fixed interval tick **only** | every `threshold` | `[aprsis] threshold` |

Notes:

- The **floor** guarantees a stationary station (or one that hasn't moved past the
  threshold) still beacons its position and telemetry at least once per `threshold`.
- With `source = "config"` only the floor applies — there is no movement trigger, so the
  station beacons position + telemetry every `threshold`.
- The movement trigger and `min_beacon_secs` cap apply to **position beacons only**;
  telemetry is never sent on a position change.

### DAO precision

The base `ddmm.hh` APRS position is quantized to hundredths of a minute (~18.5 m). The
APRS 1.2 `!DAO!` extension (WGS84) carries additional precision the base format drops;
DAO-aware clients (e.g. aprs.fi) plot the refined position while older clients ignore the
token. Choose the encoding with `[aprsis] dao` (applies to both `config` and `gpsd`
position sources):

| `dao` | Token | Added precision | Use when |
|-------|-------|-----------------|----------|
| `"human"` (default) | `!Wxy!` (`x`,`y` = `0`–`9`) | one extra digit of minutes (~1.85 m) | typical GPS; broadly legible in raw frames |
| `"base91"` | `!wxy!` (`x`,`y` = base-91) | ~1/91 of the last base digit (~0.2 m) | well-surveyed fixed station or a high-precision/RTK receiver |
| `"off"` | — | none (base `ddmm.hh` only) | maximum compatibility, or to avoid implying sub-fix precision |

Note that DAO encodes *format* precision, not accuracy — `base91` can represent ~0.2 m even
when the underlying fix is only accurate to a few metres. rtpigate is the lossless link:
the source position (e.g. GPSD's full-precision lat/lon) is preserved up to the limit of the
chosen encoding.

## Reverse Proxy Setup

The HTTP server listens on localhost only. Use a reverse proxy for public access with TLS.

### Apache

Enable required modules:

```bash
sudo a2enmod proxy proxy_http ssl headers
```

Add to your Apache virtual host config (e.g., `/etc/apache2/sites-available/aprs.conf`):

```apache
<VirtualHost *:443>
    ServerName aprs.example.com

    SSLEngine on
    SSLCertificateFile /path/to/cert.pem
    SSLCertificateKeyFile /path/to/key.pem

    ProxyPreserveHost On

    # SSE requires these to prevent buffering
    ProxyTimeout 3600
    SetEnv proxy-nokeepalive 1
    SetEnv proxy-sendchunked 1

    <Location /api/sse>
        ProxyPass http://127.0.0.1:3000/api/sse
        Header set Cache-Control "no-cache"
        Header set X-Accel-Buffering "no"
    </Location>

    # Proxy the WHOLE /api/ prefix so every backend endpoint is reachable
    # (/api/config, /api/history, /api/satellite-packets, and any added later).
    # Do NOT enumerate individual /api/* paths — a missing one silently 404s
    # at the proxy (e.g. an un-proxied /api/history leaves the activity chart
    # with no historical data).
    ProxyPass /api/ http://127.0.0.1:3000/api/
    ProxyPassReverse /api/ http://127.0.0.1:3000/api/

    # Static dashboard (served by the backend). Omit these two lines if you
    # serve the frontend files from this vhost's DocumentRoot instead.
    ProxyPass / http://127.0.0.1:3000/
    ProxyPassReverse / http://127.0.0.1:3000/
</VirtualHost>
```

```bash
sudo a2ensite aprs.conf
sudo systemctl reload apache2
```

> **Proxy the entire `/api/` prefix, not individual endpoints.** rtpigate exposes
> `/api/sse`, `/api/config`, `/api/history`, and `/api/satellite-packets`. If your
> proxy lists specific paths instead of the `/api/` prefix (or a catch-all `/`),
> any endpoint you forget will 404 at the proxy while the rest keep working —
> a missing `/api/history`, for example, lets live data through but shows no
> chart history.

### nginx

```nginx
server {
    listen 443 ssl;
    server_name aprs.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # SSE endpoint needs special buffering settings
    location /api/sse {
        proxy_pass http://127.0.0.1:3000/api/sse;
        proxy_set_header Host $host;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;
        proxy_read_timeout 3600s;
    }
}
```

## Web Dashboard

The dashboard is accessible at `http://127.0.0.1:3000` (or through your reverse proxy). It auto-connects via SSE and updates in real-time.

### Header

- **Station callsign and name** (from config)
- **Uptime timer** -- live counter since application start
- **Theme toggle** -- switches between dark and light themes (persisted in browser)
- **Status indicators**:
  - **SSE** -- green when connected, red with backoff timer when disconnected
  - **KA9Q** -- green when RF packets are being received
  - **APRS-IS** -- green when connected and receiving telemetry

### Station Configuration

Displays the active configuration (coordinates, APRS-IS settings, RTP multicast address). Enabled features are highlighted in green. Updates live on SIGHUP config reload.

### Sparkline Charts

Two groups of mini charts showing activity over the last ~25 minutes (100 data points at 15-second intervals):

**RF Packets:**
- **Direct** -- packets heard directly from the source station
- **Digipeated** -- packets received via digipeaters
- **Errors** -- packets that failed AX.25 or APRS decoding

**APRS-IS:**
- **Igated** -- RF packets successfully forwarded to APRS-IS
- **Dropped** -- packets filtered by igating rules
- **Reconnects** -- APRS-IS TCP reconnections

Hover over the `(?)` next to each label for a description. On touch devices, tap the `(?)`.

### Slicer Activity Waterfall

A heatmap that visualises how the in-process AFSK demodulator is performing. It is the most useful tuning aid in the dashboard once you understand what it shows.

**Background — what a "slicer" is.** The `aprs-rtp` demodulator does not run a single AFSK decoder; it runs a *bank* of them in parallel (8 by default). Each decoder, or **slicer**, applies a different gain to the space tone relative to the mark tone before deciding which bit was sent:

```
demod_out = mark_amplitude − (space_amplitude × gain)
```

The gains are spread geometrically across the bank (default `0.5` → `4.0`). This compensates for **audio twist** — the amplitude imbalance between the 1200 Hz mark and 2200 Hz tones that pre-emphasis (transmitter) and de-emphasis (receiver) introduce:

- **gain < 1** (low-numbered slicers) — attenuates space, so it favors **loud-space / pre-emphasized** signals.
- **gain ≈ 1** (middle slicers) — balanced, favors **flat-audio** signals.
- **gain > 1** (high-numbered slicers) — boosts space, so it favors **loud-mark / de-emphasized** signals.

A single frame is fed to every slicer at once, and any slicer that produces a CRC-valid frame "wins." Running many slicers therefore recovers packets that a single fixed decoder would miss.

**Reading the heatmap.**

- **Columns** are the individual slicers, left-to-right in increasing gain. The header under each column shows the **mark:space ratio** for that slicer (e.g. `2.0:1`, `1:1`, `1:4.0`).
- A **zone strip** above the columns groups them into **pre-emph** (green), **flat** (grey), and **de-emph** (amber) bands so you can see the twist regions at a glance.
- **Rows** are 15-second windows, **newest on top**, up to 10 rows (~2.5 minutes of history). The leftmost cell of each row is its timestamp.
- **Each cell** shows how many frames *that slicer* recovered during *that window*, with brightness scaled to the busiest cell on screen — **brighter green = more packets**. Empty cells stay dark.

**How to interpret it.**

- **Activity spread across many columns** generally means strong, clean signals — most slicers can decode them, so the frame is recovered redundantly.
- **Activity clustered on the left (pre-emph) columns** means incoming audio is loud-space — typical when receiving signals through a path that adds pre-emphasis without matching de-emphasis.
- **Activity clustered on the right (de-emph) columns** means incoming audio is loud-mark — a sign your receiver is applying de-emphasis to already-flat audio, or the transmitter is flat.
- **Activity concentrated in the middle (flat) columns** means your audio path is well balanced — the ideal case.
- **A persistent lean to one side** is a tuning hint: adjusting the receive audio de-emphasis (or the KA9Q-Radio channel's audio settings) to re-center activity toward the flat zone usually improves the overall decode rate. Slicers that never light up are effectively unused for your current signal conditions.

Hover (or tap) any cell or column header for exact counts, gains, and ratios.

### Recent Packets Table

The last 20 packets in reverse chronological order:

| Column | Description |
|--------|-------------|
| Time | Local time (HH:MM:SS) |
| Symbol | APRS symbol icon (from PNG assets) |
| Source | Callsign (links to aprs.fi) |
| Freq | Frequency in MHz (highlighted green if not 144.390) |
| Direct | T (green) if heard direct, F if digipeated |
| Satellite | T (green) if on a configured satellite frequency (`[satellite] frequencies`, default 145.825 MHz) |
| Coordinates | Lat, lon with distance from station (links to Google/Apple Maps) |
| Packet | Info field text (click to see full raw packet with address path) |

On mobile, the Direct, Satellite, and Coordinates columns are hidden to fit the screen.

### Last Heard Stations

Tracks every unique RF callsign, sorted by descending packet count:

| Column | Description |
|--------|-------------|
| Symbol | APRS symbol icon (persists from last position packet) |
| Callsign | Station callsign (links to aprs.fi) |
| Last Heard | Time of most recent packet |
| Freq | Last heard frequency |
| Last Position | Coordinates with distance (links to Google/Apple Maps) |
| Count | Total packets received from this station |

## Igating Rules

A received RF packet is **dropped** (not igated) if any of the following apply:

1. Path contains `TCPIP`, `TCPXX`, `NOGATE`, or `RFONLY`
2. Packet is a generic query (data type `?`)
3. Third-party packet (`}`) with `TCPIP` or `TCPXX` in inner header
4. Heard directly (not digipeated) on a configured satellite frequency (`[satellite] frequencies`, default 145.825 MHz) unless the source is a known satellite (`RS0ISS`, `NA1SS`, `DP0ISS`, `OR4ISS`, `IR0ISS`, `DP0SNX`)
5. Packet age exceeds 30 seconds (staleness guard)
6. Duplicate of a packet igated within the last 30 seconds (same source + info)
7. No valid passcode configured

## Operations

### Viewing Logs

```bash
# Follow live logs
journalctl -u rtpigate -f

# Last 100 lines
journalctl -u rtpigate -n 100

# Since last boot
journalctl -u rtpigate -b
```

### Reloading Configuration

Edit the config file and send SIGHUP -- no restart needed:

```bash
sudo nano /etc/rtpigate/config.toml
sudo systemctl reload rtpigate
```

The updated config is pushed to connected browsers immediately via SSE. Note: changes to RTP host/port or APRS-IS host/port require a full restart.

### Service Management

```bash
sudo systemctl start rtpigate
sudo systemctl stop rtpigate
sudo systemctl restart rtpigate
sudo systemctl status rtpigate
```

### Building a Release

```bash
# Build .deb and publish to GitHub Releases
./deploy/release.sh 0.1.0
```

This tags the version, builds the `.deb` package via `cargo-deb`, and creates a GitHub release with the package attached.

## Key Dependencies

The RF receive path is built on two crates from crates.io (no vendored fork is required anymore):

- **[`aprs-rtp`](https://crates.io/crates/aprs-rtp)** — subscribes to a KA9Q-Radio RTP audio multicast group, de-jitters it, demodulates 1200-baud AFSK through the parallel slicer bank, frames HDLC, validates CRC (single-bit error recovery by default), and parses AX.25. Decoder defaults: 8 slicers, space-gain ladder `0.5`–`4.0`, 2-packet jitter buffer.
- **[`aprs-decode`](https://crates.io/crates/aprs-decode)** — parses the APRS information field (position, Mic-E, object, item, altitude, and map symbol) from each decoded frame.

Other notable dependencies: `tokio` (async runtime), `axum` + `tower-http` (HTTP/SSE server), `serde`/`serde_json` (telemetry serialization), and `chrono` (timestamps).

## License

GPL-3.0
