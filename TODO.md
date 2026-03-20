# TODO - Improvement Ideas

## Reliability
- [x] Graceful shutdown message to APRS-IS — clean TCP shutdown on cancellation
- [x] Duplicate packet suppression — 30s TTL dedup cache on source:info key
- [x] Configurable listen address — via [http] listen in config.toml
- [x] Fix igating filters: add TCPXX to rfonly, add query (?) drop, fix third-party (}) check

## Observability
- [ ] Uptime display — show how long the application has been running in the config panel
- [ ] Last-heard table — track unique callsigns with last-heard time, frequency, direct/digi status
- [ ] APRS-IS connection state in telemetry — current connection duration and total reconnect count

## Frontend
- [x] Mic-E position decoding — done via aprs-parser-rs on backend
- [ ] Packet type filtering — toggles to show/hide RF vs internet packets, or filter by frequency
- [ ] Dark/light theme toggle — CSS variable swap
- [ ] Responsive mobile layout — card-based layout for narrow screens

## Operational
- [ ] Systemd service file — .service unit for running as a daemon on the Pi with auto-restart
- [ ] Config file validation — validate all required fields at startup with clear error messages
- [ ] Signal-based config reload — SIGHUP to re-read config.toml without restarting

## Code Quality
- [ ] Tests — unit tests for droppacket(), positpacket(), passcode(), rfonly/heard-direct detection, telemetry encoding
- [ ] Error types — replace Box<dyn Error> with a proper enum via thiserror
