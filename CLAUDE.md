# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rtpigate is a receive-only APRS igate written in Rust. It subscribes to the decoded-APRS multicast stream published by the `aprs-streamd` base service (see https://github.com/deatojef/aprs-stream) — fully-decoded, typed `AprsFrame` messages via the shared `aprs-stream` crate — filters them, and gates qualifying packets to the APRS-IS internet network. The RTP audio, AFSK demodulation, and AX.25/APRS decode happen once, upstream in `aprs-streamd`; rtpigate no longer touches any of that. A web frontend displays real-time packet data via Server-Sent Events.

## Build & Run Commands

```bash
cargo build          # Debug build
cargo build --release # Release build
cargo run            # Run with config.toml in project root
cargo check          # Type-check without building
cargo clippy         # Lint
cargo test           # Run tests (currently no test suite)
```

The application reads `config.toml` from the current working directory at startup.

## Architecture

**Async actor model using tokio with broadcast channels for inter-task communication.**

Up to five concurrent tasks orchestrated in `main.rs`:

1. **rtp_listener** (`stream.rs`) — Joins the decoded-APRS UDP multicast group via `aprs_stream::Subscriber`, maps each typed `AprsFrame` to the internal `RTPPacket` (AX.25 framing facts read from the frame's `ax25_meta`; payload from `frame.parsed` — nothing re-parsed), and broadcasts `DataItem::Pkt` on a shared channel (capacity 128). The function keeps its historical name.

2. **aprsis_task** (`aprsis.rs`) — Maintains persistent TCP connection to APRS-IS server with capped exponential backoff (5s→300s max). Consumes RF packets from broadcast channel, applies igating filter rules, and forwards to APRS-IS. Sends position beacons and telemetry on a configurable interval.

3. **sse_task** (`sse.rs`) — Consumes `DataItem` packets and telemetry, serializes to JSON, pushes to an SSE broadcast channel (capacity 16). Event types: `rfpacket`, `inetpacket`, `packet_statistics`, `aprsis_statistics`, `gps_status`.

4. **gpsd_task** (`gpsd.rs`) — Only spawned when `[location] source = "gpsd"`. Connects to gpsd over TCP (same backoff pattern as aprsis_task), reads newline-delimited JSON `TPV`/`SKY` reports, and keeps a shared `Arc<RwLock<Option<GpsFix>>>` updated for aprsis_task to source beacon position. Also broadcasts `gps_status` telemetry for the frontend.

5. **HTTP server** (axum) — Listens on `127.0.0.1:3000`, serves `/sse` endpoint. Intended to sit behind Apache reverse proxy.

Graceful shutdown via `CancellationToken` on SIGTERM/SIGINT.

## Key Igating Rules (aprsis.rs)

- Drops packets with TCPIP, TCPXX, NOGATE, or RFONLY in path
- Drops generic queries (`?`) and third-party packets (`}`) with internet markers
- Drops satellite packets (145.825 MHz)
- Drops packets older than 30 seconds (staleness guard)
- Uses `qAO` construct (receive-only igate)

## Configuration (config.toml)

Sections: `[station]` (callsign), `[location]` (lat/lon/alt + `source`), `[aprsis]` (server, passcode, beacon/igate flags, symbol, telemetry interval, `dao` precision), `[stream]` (decoded-APRS multicast group/port, optional interface + recv buffer), `[gpsd]` (GPSD connection + movement beaconing). Config traits `APRSISLogin` and `APRSISPasscode` are in `config.rs`.

### Beacon DAO precision (`[aprsis] dao`)

`DaoMode` controls the APRS 1.2 `!DAO!` precision token in `igate::positpacket()`: `"human"` (default, `!Wxy!`, ~1.85 m), `"base91"` (`!wxy!`, ~0.2 m), or `"off"` (base `ddmm.hh` only). The base position truncates to hundredths of a minute and the remainder feeds the token.

### Position source (`[location] source`)

- `source = "config"` (default) — beacon the fixed `[location]` lat/lon/alt (the RF-antenna location).
- `source = "gpsd"` — beacon a live fix from gpsd. Behavior:
  - **No-fix skip**: a position beacon is skipped when there is no fresh fix (2D/3D, newer than `GPS_FRESHNESS` = 30s); telemetry still sends. A `GpsFix` is "fresh" per `GpsFix::is_fresh()` in `config.rs`.
  - **Movement beaconing**: a second timer (`gpsd.min_beacon_secs`, default 30s) beacons when lat/lon moves past `gpsd.move_threshold_deg` (default 0.0001°) since the last beacon — but never faster than `min_beacon_secs`. The fixed `aprsis.threshold` interval remains the upper floor (always beacons + telemetry at least that often when a fresh fix exists).

## Vendored Dependency

`vendor/aprs-parser-rs/` — Local fork of aprs-parser-rs v0.4.2 for APRS packet parsing. Referenced as a path dependency in Cargo.toml.

## Frontend

`frontend/assets/` contains APRS symbol PNGs and `symbols-map.js` for symbol code→image mapping. The frontend connects to `/sse` for real-time updates. Design spec is in `thoughts.md`.
