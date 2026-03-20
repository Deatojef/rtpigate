# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rtpigate is an APRS igate application for KA9Q-Radio backend, written in Rust. It receives AX.25 frames via RTP multicast from KA9Q-Radio, filters them, and gates them to the APRS-IS internet network. A web frontend displays real-time packet data via Server-Sent Events.

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

Four concurrent tasks orchestrated in `main.rs`:

1. **rtp_listener** (`packet.rs`) — Binds to UDP multicast, parses RTP headers and AX.25 frames, broadcasts `DataItem::Pkt` on a shared channel (capacity 128).

2. **aprsis_task** (`aprsis.rs`) — Maintains persistent TCP connection to APRS-IS server with capped exponential backoff (5s→300s max). Consumes RF packets from broadcast channel, applies igating filter rules, and forwards to APRS-IS. Sends position beacons and telemetry on a configurable interval.

3. **sse_task** (`sse.rs`) — Consumes `DataItem` packets and telemetry, serializes to JSON, pushes to an SSE broadcast channel (capacity 16). Event types: `rfpacket`, `inetpacket`, `packet_statistics`, `aprsis_statistics`.

4. **HTTP server** (axum) — Listens on `127.0.0.1:3000`, serves `/sse` endpoint. Intended to sit behind Apache reverse proxy.

Graceful shutdown via `CancellationToken` on SIGTERM/SIGINT.

## Key Igating Rules (aprsis.rs)

- Drops packets with TCPIP, TCPXX, NOGATE, or RFONLY in path
- Drops generic queries (`?`) and third-party packets (`}`) with internet markers
- Drops satellite packets (145.825 MHz)
- Drops packets older than 30 seconds (staleness guard)
- Uses `qAO` construct (receive-only igate)

## Configuration (config.toml)

Sections: `[station]` (callsign), `[location]` (lat/lon/alt), `[aprsis]` (server, passcode, beacon/igate flags, symbol, telemetry interval), `[rtp]` (multicast host/port). Config traits `APRSISLogin` and `APRSISPasscode` are in `config.rs`.

## Vendored Dependency

`vendor/aprs-parser-rs/` — Local fork of aprs-parser-rs v0.4.2 for APRS packet parsing. Referenced as a path dependency in Cargo.toml.

## Frontend

`frontend/assets/` contains APRS symbol PNGs and `symbols-map.js` for symbol code→image mapping. The frontend connects to `/sse` for real-time updates. Design spec is in `thoughts.md`.
