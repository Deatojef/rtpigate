# TODO - Improvement Ideas

## Reliability
- [x] Graceful shutdown message to APRS-IS — clean TCP shutdown on cancellation
- [x] Duplicate packet suppression — 30s TTL dedup cache on source:info key
- [x] Configurable listen address — via [http] listen in config.toml
- [x] Fix igating filters: add TCPXX to rfonly, add query (?) drop, fix third-party (}) check

## Observability
- [x] Uptime display — live timer in header from backend start time
- [x] Last-heard table — tracks unique RF callsigns with time, freq, direct/via, count
- [x] APRS-IS connection state — reconnect counter with sparkline in stats panel

## Frontend
- [x] Mic-E position decoding — done via aprs-parser-rs on backend
- [x] Packet type filtering — All/RF/Inet toggle buttons
- [x] Dark/light theme toggle — CSS variables with localStorage persistence
- [x] Responsive mobile layout — hides non-essential columns on narrow screens

## Operational
- [x] Systemd service file — .service unit with install script, security hardening, SIGHUP reload
- [x] Config file validation — validate all required fields at startup with clear error messages
- [x] Signal-based config reload — SIGHUP re-reads and validates config.toml, updates frontend config

## Code Quality
- [x] Tests — 44 unit tests covering passcode, login, config validation, droppacket, positpacket, telemetry, quadratic encoding
- [x] Error types — RtpigateError enum with Network, Io, Parse, Config, Validation variants
