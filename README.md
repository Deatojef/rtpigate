# rtpigate

An APRS receive-only iGate for [KA9Q-Radio](http://www.ka9q.net/radio/) backend, written in Rust. Receives AX.25 frames via RTP multicast, filters them per APRS-IS igating rules, and forwards qualifying packets to the APRS-IS internet network. Includes a real-time web dashboard for monitoring station activity.

## Features

- **Receive-only iGate** (RF to Internet) using `qAO` construct
- **Multi-frequency support** via KA9Q-Radio's RTP multicast stream
- **Position beaconing** and **APRS telemetry** to APRS-IS
- **Real-time web dashboard** with Server-Sent Events (SSE)
  - Live packet table with APRS symbol icons, coordinates, and distance
  - Last-heard stations table sorted by packet count
  - Sparkline charts for RF and APRS-IS statistics
  - Callsign links to aprs.fi, coordinate links to Google/Apple Maps
  - Dark/light theme toggle
  - Mobile-responsive layout
- **Resilient networking** with capped exponential backoff on all connections
- **Duplicate packet suppression** (30-second TTL dedup cache)
- **SIGHUP config reload** with instant SSE push to connected browsers
- **Systemd integration** with security hardening and dedicated service user
- **Debian packaging** via `cargo-deb` for easy deployment

## Architecture

Four concurrent async tasks orchestrated with tokio:

```
                                                                     +-----------+
+-----------+   RTP/UDP    +----------+  broadcast  +--------+  SSE  |   axum    |
| KA9Q-Radio| (multicast)  | ka9q.rs  | (DataItem)  | sse.rs | ----> | /api/sse  | --> Browser
|   (SDR)   | -----------> |          | ----------> |        |       +-----------+
+-----------+              +----------+             +--------+
                                |
                                | broadcast (DataItem)
                                v
                           +----------+   TCP    +-----------+
                           | igate.rs | -------> | aprs_is.rs| --> APRS-IS
                           | (filter) |          | (connect) |
                           +----------+          +-----------+
```

- **ka9q.rs** -- Binds to UDP multicast, parses RTP headers and AX.25 frames, extracts APRS packets. Reconnects with backoff on socket failure.
- **aprs_is.rs** -- Maintains persistent TCP connection to APRS-IS with capped exponential backoff (5s to 300s). Handles login verification, beaconing, and telemetry.
- **igate.rs** -- Applies igating filter rules (rfonly, staleness, satellite, query, third-party, duplicates).
- **sse.rs** -- Serializes packets and telemetry to JSON, pushes to SSE broadcast channel.
- **axum HTTP server** -- Serves the web dashboard and SSE endpoint. Sits behind a reverse proxy.

Graceful shutdown via `CancellationToken` on SIGTERM/SIGINT with clean TCP FIN to APRS-IS.

## Requirements

- **Rust** 2024 edition (1.85+)
- **KA9Q-Radio** running and producing RTP multicast AX.25 frames
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
lat = 30.123456                # Decimal degrees, -90 to 90. Required for beaconing.
lon = -99.123456              # Decimal degrees, -180 to 180. Required for beaconing.
alt = 1234                     # Altitude in feet. Required for beaconing.

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

[rtp]
host = "ax25.local"            # KA9Q-Radio multicast hostname or IP. Required.
port = 5004                    # KA9Q-Radio multicast UDP port. Required.

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
- Beaconing is enabled but location (lat, lon, alt) is incomplete
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
    ProxyPass / http://127.0.0.1:3000/
    ProxyPassReverse / http://127.0.0.1:3000/

    # SSE requires these to prevent buffering
    ProxyTimeout 3600
    SetEnv proxy-nokeepalive 1
    SetEnv proxy-sendchunked 1

    <Location /api/sse>
        ProxyPass http://127.0.0.1:3000/api/sse
        Header set Cache-Control "no-cache"
        Header set X-Accel-Buffering "no"
    </Location>
</VirtualHost>
```

```bash
sudo a2ensite aprs.conf
sudo systemctl reload apache2
```

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

### Recent Packets Table

The last 20 packets in reverse chronological order:

| Column | Description |
|--------|-------------|
| Time | Local time (HH:MM:SS) |
| Symbol | APRS symbol icon (from PNG assets) |
| Source | Callsign (links to aprs.fi) |
| Freq | Frequency in MHz (highlighted green if not 144.390) |
| Direct | T (green) if heard direct, F if digipeated |
| Satellite | T (green) if on 145.825 MHz |
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
4. Heard directly on satellite frequency (145.825 MHz) unless from a known satellite (`RS0ISS`, `DP0SNX`, `A55BTN`)
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

## Vendored Dependencies

`vendor/aprs-parser-rs/` is a local fork of [aprs-parser-rs](https://github.com/Turbo87/aprs-parser-rs) v0.4.2 with altitude parsing fixes. It is referenced as a path dependency in `Cargo.toml`.

## License

MIT
