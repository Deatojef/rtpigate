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
- [ ] Systemd service file — .service unit for running as a daemon on the Pi with auto-restart
- [ ] Config file validation — validate all required fields at startup with clear error messages
- [ ] Signal-based config reload — SIGHUP to re-read config.toml without restarting

## Code Quality
- [ ] Tests — unit tests for droppacket(), positpacket(), passcode(), rfonly/heard-direct detection, telemetry encoding
- [ ] Error types — replace Box<dyn Error> with a proper enum via thiserror
