use std::sync::{Arc, RwLock};
use std::time::Instant;

use chrono::{DateTime, Local, Utc};
use log::{debug, info, warn};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::sync::CancellationToken;

use gpsd_proto::{ENABLE_WATCH_CMD, Mode, UnifiedResponse};

use crate::config::{AppTelemetry, Config, DataItem, GpsFix, GpsTelemetry};
use crate::error::RtpigateError;

// Connection-level timeouts, mirroring the APRS-IS task.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

const METERS_TO_FEET: f64 = 3.28084;

/// GPSD client task. Maintains a persistent connection to gpsd, parses the
/// newline-delimited JSON `TPV` (position) and `SKY` (satellite) reports, and
/// keeps `gps_state` updated with the latest fix. Each update is also broadcast
/// as `AppTelemetry::GpsStatus` for the web frontend.
///
/// Spawned only when `[location] source = "gpsd"`. Fully async and non-blocking;
/// reconnects with capped exponential backoff like `aprsis_task`.
pub async fn gpsd_task(
    data_channel: broadcast::Sender<DataItem>,
    token: CancellationToken,
    config: Arc<Config>,
    gps_state: Arc<RwLock<Option<GpsFix>>>,
) -> Result<(), RtpigateError> {
    info!("Started");

    let gpsd = config.gpsd_config();
    let host = gpsd.host();
    let port = gpsd.port();
    let address = format!("{}:{}", host, port);

    // backoff state for reconnection
    let mut backoff_secs: u64 = 5;
    const MAX_BACKOFF_SECS: u64 = 300;

    // accumulated fix fields — TPV and SKY arrive as separate messages, so we
    // merge them into a single rolling fix. `received_at` tracks position freshness
    // and is only refreshed on TPV (position) reports.
    let mut lat: Option<f64> = None;
    let mut lon: Option<f64> = None;
    let mut alt_ft: Option<f64> = None;
    let mut mode: u8 = 1;
    let mut time: Option<DateTime<Utc>> = None;
    let mut sats_used: u32 = 0;
    let mut sats_visible: u32 = 0;
    let mut hdop: Option<f64> = None;
    let mut received_at: Instant = Instant::now();

    // last reported fix mode, so we can log a single INFO line on a state change
    // (no fix <-> 2D <-> 3D) rather than on every report.
    let mut prev_mode: Option<u8> = None;

    // gpsd can emit many reports per second; the shared fix is updated on every
    // one (cheap, keeps beaconing fresh), but the `gps_status` telemetry is
    // throttled to this cadence so it doesn't flood the broadcast/SSE channels
    // (which also carry RF packets). A fix-state change emits immediately.
    const GPS_TELEM_INTERVAL: Duration = Duration::from_secs(1);
    let mut last_telem: Option<Instant> = None;

    // outer connection loop
    loop {
        if token.is_cancelled() {
            break;
        }

        // resolve the address asynchronously (non-blocking DNS)
        let mut addrs = match tokio::net::lookup_host(&address).await {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    "GPSD: unable to resolve {}: {}. Retrying in {}s...",
                    address, e, backoff_secs
                );
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
        };

        let sock_addr = match addrs.next() {
            Some(a) => a,
            None => {
                warn!(
                    "GPSD: no socket address for {}. Retrying in {}s...",
                    address, backoff_secs
                );
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
        };

        // connect (bounded timeout)
        let socket: TcpStream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(sock_addr)).await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!(
                    "GPSD: failed to connect to {}: {}. Retrying in {}s...",
                    address, e, backoff_secs
                );
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
            Err(_elapsed) => {
                warn!(
                    "GPSD: connect to {} timed out. Retrying in {}s...",
                    address, backoff_secs
                );
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
        };

        // detect silently-dropped connections at the OS level
        {
            let ka = TcpKeepalive::new()
                .with_time(Duration::from_secs(60))
                .with_interval(Duration::from_secs(20));
            if let Err(e) = SockRef::from(&socket).set_tcp_keepalive(&ka) {
                warn!("GPSD: failed to set TCP keepalive: {}", e);
            }
        }

        info!("GPSD: connected to {}", address);
        backoff_secs = 5;

        let (read_half, mut write_half) = socket.into_split();

        // enable JSON streaming
        match timeout(
            WRITE_TIMEOUT,
            write_half.write_all(ENABLE_WATCH_CMD.as_bytes()),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("GPSD: failed to send WATCH: {}. Reconnecting...", e);
                continue;
            }
            Err(_elapsed) => {
                warn!("GPSD: WATCH write timed out. Reconnecting...");
                continue;
            }
        }

        let mut lines = BufReader::new(read_half).lines();

        // inner read loop
        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok(()),

                next = lines.next_line() => {
                    let line = match next {
                        Ok(Some(l)) => l,
                        Ok(None) => {
                            warn!("GPSD: connection closed by server. Reconnecting...");
                            break;
                        },
                        Err(e) => {
                            warn!("GPSD: read error: {}. Reconnecting...", e);
                            break;
                        },
                    };

                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<UnifiedResponse>(trimmed) {
                        Ok(UnifiedResponse::Tpv(tpv)) => {
                            mode = match tpv.mode {
                                Mode::NoFix => 1,
                                Mode::Fix2d => 2,
                                Mode::Fix3d => 3,
                            };
                            lat = tpv.lat;
                            lon = tpv.lon;
                            // prefer MSL altitude; fall back to the generic `alt` (both meters)
                            alt_ft = tpv.alt_msl.or(tpv.alt).map(|m| m as f64 * METERS_TO_FEET);
                            time = tpv.time.as_deref()
                                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                                .map(|dt| dt.with_timezone(&Utc));
                            received_at = Instant::now();
                        },
                        Ok(UnifiedResponse::Sky(sky)) => {
                            hdop = sky.hdop.map(|h| h as f64);
                            if let Some(sats) = &sky.satellites {
                                sats_visible = sats.len() as u32;
                                sats_used = sats.iter().filter(|s| s.used).count() as u32;
                            } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                // Some gpsd builds emit a summary SKY with scalar
                                // uSat (used) / nSat (seen) instead of a satellites
                                // array — gpsd_proto doesn't surface those, so read
                                // them from the raw JSON.
                                if let Some(n) = v.get("uSat").and_then(|x| x.as_u64()) {
                                    sats_used = n as u32;
                                }
                                if let Some(n) = v.get("nSat").and_then(|x| x.as_u64()) {
                                    sats_visible = n as u32;
                                }
                            }
                        },
                        Ok(_) => continue, // VERSION/WATCH/DEVICE/etc. — not needed
                        Err(e) => {
                            debug!("GPSD: failed to parse line ({}): {}", e, trimmed);
                            continue;
                        },
                    }

                    // merge into the current fix
                    let fix = GpsFix {
                        lat, lon, alt_ft, mode, sats_used, sats_visible, hdop, time, received_at,
                    };

                    // log once when the fix state changes (e.g. acquired a 3D fix,
                    // or lost the fix entirely)
                    let mode_changed = prev_mode != Some(fix.mode);
                    if mode_changed {
                        let label = match fix.mode {
                            3 => "3D fix",
                            2 => "2D fix",
                            _ => "no fix",
                        };
                        match (fix.lat, fix.lon) {
                            (Some(la), Some(lo)) =>
                                info!("GPS fix changed: {} ({:.5}, {:.5}, {} sats used)", label, la, lo, fix.sats_used),
                            _ => info!("GPS fix changed: {} ({} sats used)", label, fix.sats_used),
                        }
                        prev_mode = Some(fix.mode);
                    }

                    // Push the frontend telemetry on the throttled cadence (or
                    // immediately on a state change), built from the fix so the
                    // shared state stays the single source of truth.
                    if mode_changed || last_telem.is_none_or(|t| t.elapsed() >= GPS_TELEM_INTERVAL) {
                        let telem = GpsTelemetry {
                            name: String::from("gps_status"),
                            timestamp: Local::now(),
                            lat: fix.lat,
                            lon: fix.lon,
                            alt_ft: fix.alt_ft,
                            mode: fix.mode,
                            sats_used: fix.sats_used,
                            sats_visible: fix.sats_visible,
                            hdop: fix.hdop,
                            time: fix.time,
                            fresh: fix.is_fresh(),
                        };
                        let _ = data_channel.send(DataItem::Tlm(AppTelemetry::GpsStatus(telem)));
                        last_telem = Some(Instant::now());
                    }

                    // always update the shared fix (cheap; keeps beaconing position fresh)
                    if let Ok(mut guard) = gps_state.write() {
                        *guard = Some(fix);
                    }
                },
            }
        }
    }

    info!("Task ended.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Confirms the gpsd_proto deserialization contract this module relies on:
    // a TPV line yields position + a 3D mode, and altMSL maps to meters.
    #[test]
    fn parses_tpv_position() {
        let line = r#"{"class":"TPV","device":"/dev/ttyAMA0","mode":3,"time":"2026-06-26T14:51:51.000Z","lat":40.123456,"lon":-103.123456,"altMSL":381.0}"#;
        match serde_json::from_str::<UnifiedResponse>(line).expect("parse TPV") {
            UnifiedResponse::Tpv(tpv) => {
                assert!(matches!(tpv.mode, Mode::Fix3d));
                assert_eq!(tpv.lat, Some(40.123456));
                assert_eq!(tpv.lon, Some(-103.123456));
                // altMSL in meters -> feet conversion used by the task
                let alt_ft = tpv.alt_msl.unwrap() as f64 * METERS_TO_FEET;
                assert!((alt_ft - 381.0 * METERS_TO_FEET).abs() < 0.01);
            }
            _ => panic!("expected TPV"),
        }
    }

    // Confirms SKY parsing: hdop and the used-satellite count this module exposes.
    #[test]
    fn parses_sky_satellites() {
        let line = r#"{"class":"SKY","device":"/dev/ttyAMA0","hdop":0.9,"satellites":[{"PRN":1,"el":45,"az":100,"ss":40,"used":true},{"PRN":2,"el":10,"az":200,"ss":20,"used":false},{"PRN":3,"el":30,"az":300,"ss":35,"used":true}]}"#;
        match serde_json::from_str::<UnifiedResponse>(line).expect("parse SKY") {
            UnifiedResponse::Sky(sky) => {
                assert_eq!(sky.hdop, Some(0.9));
                let sats = sky.satellites.unwrap();
                assert_eq!(sats.len(), 3);
                assert_eq!(sats.iter().filter(|s| s.used).count(), 2);
            }
            _ => panic!("expected SKY"),
        }
    }

    #[test]
    fn ignores_non_position_classes() {
        let line =
            r#"{"class":"VERSION","release":"3.22","rev":"3.22","proto_major":3,"proto_minor":14}"#;
        let resp = serde_json::from_str::<UnifiedResponse>(line).expect("parse VERSION");
        assert!(!matches!(
            resp,
            UnifiedResponse::Tpv(_) | UnifiedResponse::Sky(_)
        ));
    }
}
