use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{tcp, TcpStream}, sync::broadcast, time::{interval, timeout, Duration}};
use tokio_util::sync::CancellationToken;
use std::{collections::{HashMap, VecDeque}, sync::{Arc, RwLock}};
use chrono::{Local, Utc};
use socket2::{SockRef, TcpKeepalive};

use log::{info, warn, debug};
use tokio::time::sleep;

// Connection-level timeouts. APRS-IS servers send `#` keepalive comments every ~20s,
// so writes/login should complete well within these bounds on a healthy link.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

use crate::config::{Config, APRSISLogin, APRSISPasscode, AppTelemetry, DataSeries, DataPoint, AprsisTelemetry, DataItem, GpsFix, Location, PositionSource, DaoMode};
use crate::error::RtpigateError;
use crate::ka9q::Packet;
use crate::igate::{self, TOCALL, AnalogItem, APRSQuadratic, Telemetry};


/// Builds and writes a position beacon for `loc` to the APRS-IS server. Returns
/// `true` if the connection is still healthy (the beacon was sent, or skipped due
/// to an invalid position), and `false` if the write failed/timed out and the
/// caller should reconnect.
async fn send_position_beacon(
    write_half: &mut tcp::OwnedWriteHalf,
    loc: &Location,
    callsign: &str,
    name: &str,
    symbol: &Option<String>,
    overlay: &Option<String>,
    dao: DaoMode,
) -> bool {
    let posit_text = match igate::positpacket(loc, callsign, name, symbol, overlay, dao) {
        Ok(p) => p,
        Err(e) => {
            warn!("Error creating position packet: {}. Skipping beacon.", e);
            return true; // not a connection failure — keep the connection
        },
    };

    info!("xmitting: {}", posit_text);
    match timeout(WRITE_TIMEOUT, write_half.write_all(format!("{}\r\n", posit_text).as_bytes())).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("Write to APRS-IS failed: {}", e);
            false
        },
        Err(_elapsed) => {
            warn!("Write to APRS-IS timed out after {}s, reconnecting", WRITE_TIMEOUT.as_secs());
            false
        },
    }
}

/// Resolves the beacon location from the latest GPSD fix, or `None` if there is
/// no fresh fix. Altitude falls back to the configured value when the fix lacks one.
fn gpsd_beacon_location(gps_state: &RwLock<Option<GpsFix>>, fallback_alt: Option<f64>) -> Option<Location> {
    let guard = gps_state.read().ok()?;
    let fix = guard.as_ref()?;
    if fix.is_fresh() {
        Some(Location {
            lat: fix.lat,
            lon: fix.lon,
            alt: fix.alt_ft.or(fallback_alt),
            source: PositionSource::Gpsd,
        })
    } else {
        None
    }
}


/// Main APRS-IS task: manages the TCP connection and coordinates igating, beaconing, and telemetry.
pub async fn aprsis_task(data_channel: broadcast::Sender<DataItem>, token: CancellationToken, config: Arc<Config>, gps_state: Arc<RwLock<Option<GpsFix>>>) -> Result<(), RtpigateError> {

    info!("Started");

    // subscribe to the channels
    let mut data_stream = data_channel.subscribe();

    // counters for telemetry packets sent to APRS-IS
    let mut rf_received: u32 = 0;
    let mut dropped: u32 = 0;
    let mut heard_direct: u32 = 0;
    let mut received_sat: u32 = 0;
    let mut received_other: u32 = 0;

    // counters for app telemetry
    let mut packets_dropped: u32 = 0;
    let mut packets_igated: u32 = 0;
    let mut stats_rf_received: u32 = 0;

    // lifetime counters (never reset)
    let mut lifetime_rf_received: u64 = 0;
    let mut lifetime_packets_igated: u64 = 0;
    let mut lifetime_packets_dropped: u64 = 0;

    // per-reason drop breakdown (lifetime). Each filter rule increments exactly
    // one of these; their sum (plus duplicates) equals lifetime_packets_dropped.
    let mut lifetime_drops_stale: u64 = 0;
    let mut lifetime_drops_rfonly: u64 = 0;
    let mut lifetime_drops_query: u64 = 0;
    let mut lifetime_drops_thirdparty: u64 = 0;
    let mut lifetime_drops_sat: u64 = 0;
    let mut lifetime_drops_duplicate: u64 = 0;
    let mut lifetime_drops_malformed: u64 = 0;

    // packets that were dropped by the broadcast channel before reaching gating
    // (RecvError::Lagged). These never had a chance to be evaluated.
    let mut lifetime_lagged_drops: u64 = 0;

    // data series
    let mut rf_received_series = DataSeries { name: String::from("rf_received"), data: VecDeque::new() };
    let mut dropped_series = DataSeries { name: String::from("packets_dropped"), data: VecDeque::new() };
    let mut igated_series = DataSeries { name: String::from("packets_igated"), data: VecDeque::new() };
    let mut reconnect_series = DataSeries { name: String::from("aprsis_reconnects"), data: VecDeque::new() };

    // the interval for when to send statistics
    let mut telemetry_interval = interval(Duration::from_secs(15));


    // get the hostname of the APRS-IS host
    let host: String = match &config.aprsis.host {
        Some(h) => h.clone(),
        None => {
            return Err(RtpigateError::Config("Unable to determine APRS-IS host".into()));
        },
    };

    // get the port number of the connection to the APRS-IS host
    let port: u32 = match &config.aprsis.port {
        Some(p) => *p,
        None => {
            return Err(RtpigateError::Config("Unable to determine APRS-IS port number".into()));
        },
    };

    // construct the hostname:port string
    let address = format!("{}:{}", &host, &port);

    // the login string used when connecting to aprs-is
    let loginstring = config.aprsis_login_string();

    // get this station's callsign
    let callsign: String = match &config.station.callsign {
        Some(c) => c.clone(),
        None => {
            return Err(RtpigateError::Config("Unable to determine station callsign".into()));
        },
    };

    // is beaconing enabled?
    let beaconing: bool = match &config.aprsis.beaconing {
        Some(b) => *b,
        None => false,
    };

    // is igating enabled?
    let igating: bool = match &config.aprsis.igating {
        Some(i) => *i,
        None => false,
    };

    // does this station have a name?
    let station_name: String = match &config.station.name {
        Some(n) => n.clone(),
        None => format!("{}'s station", callsign),
    };

    // symbol and overlay
    let symbol = config.aprsis.symbol.clone();
    let overlay = config.aprsis.overlay.clone();

    // !DAO! precision mode for position beacons
    let dao_mode = config.dao_mode();

    // station location and where the beacon position comes from
    let location = config.location.clone();
    let position_source = config.location.source;

    // movement-beaconing parameters (only used when source == Gpsd)
    let gpsd_cfg = config.gpsd_config();
    let move_threshold_deg = gpsd_cfg.move_threshold_deg();
    let min_beacon_secs = gpsd_cfg.min_beacon_secs();

    // lat/lon last actually beaconed — drives the movement trigger and avoids
    // duplicate beacons. Updated whenever a position beacon is sent.
    let mut last_beacon_pos: Option<(f64, f64)> = None;

    // is this connection to the APRS-IS server read-only (i.e. passcode = -1) or read-write with a valid passcode?
    let rw = config.passcode_isvalid();

    // threshold in secs of how often we need to send telemetry data
    let telemetry_threshold = match config.aprsis.threshold {
        Some(n) => n,
        None => 600,
    };

    match rw {
        true => {
            info!("Beaconing: {}, Igating: {}", beaconing, igating);
            info!("Posit & Telemetry threshold: {}", telemetry_threshold);
        },
        false => info!("Aprsis passcode is invalid.  Igating and beaconing to aprsis is disabled."),
    }

    // interval object for triggering telemetry/beacon packets to APRS-IS
    let mut time_interval = interval(Duration::from_secs(telemetry_threshold));

    // movement-evaluation interval for the GPSD source. Its period is also the
    // minimum spacing between movement-triggered beacons, so a fast mover can
    // never beacon faster than `min_beacon_secs`. Harmless when source != Gpsd.
    let mut move_interval = interval(Duration::from_secs(min_beacon_secs));

    // telemetry sequence filename
    let telemetry_file = "/tmp/telem-seq.txt";

    // sequence number
    let mut sequence: u32 = igate::read_telemetry_file(telemetry_file).await?;


    // duplicate packet suppression: maps "source:info" to the time it was last igated
    let mut dedup_cache: HashMap<String, i64> = HashMap::new();
    const DEDUP_TTL_SECS: i64 = 30;

    // APRS-IS connection tracking
    let mut total_reconnects: u32 = 0;
    let mut reconnects_this_interval: u32 = 0;
    let mut first_connect = true;

    // backoff state for reconnection
    let mut backoff_secs: u64 = 5;
    const MAX_BACKOFF_SECS: u64 = 300;

    // outer aprs-is connection loop
    loop {

        // check if the main program has requested a shutdown
        if token.is_cancelled() {
            break;
        }

        // resolve the address asynchronously (non-blocking DNS)
        let mut addrs = match tokio::net::lookup_host(&address).await {
            Ok(a) => a,
            Err(e) => {
                warn!("Unable to resolve address, {}. {}. Retrying in {}s...", address, e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        let sock_addr = match addrs.next() {
            Some(a) => a,
            None => {
                warn!("No valid socket address found for {}. Retrying in {}s...", address, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        // create a TCP stream (with bounded connect timeout)
        let socket: TcpStream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(sock_addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!("Failed to connect to {}:{}: {}. Retrying in {}s...", host, port, e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
            Err(_elapsed) => {
                warn!("Connect to {}:{} timed out after {}s. Retrying in {}s...", host, port, CONNECT_TIMEOUT.as_secs(), backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        // Enable TCP keepalive so a half-open connection (silent network drop, NAT
        // idle, hung server) is detected at the OS level rather than blocking writes
        // forever.
        {
            let ka = TcpKeepalive::new()
                .with_time(Duration::from_secs(60))
                .with_interval(Duration::from_secs(20));
            if let Err(e) = SockRef::from(&socket).set_tcp_keepalive(&ka) {
                warn!("Failed to set TCP keepalive on APRS-IS socket: {}", e);
            }
        }

        // reset backoff on successful connection
        backoff_secs = 5;

        // track reconnects (skip the initial connection)
        if first_connect {
            first_connect = false;
        } else {
            total_reconnects += 1;
            reconnects_this_interval += 1;
            info!("APRS-IS reconnected (total reconnects: {})", total_reconnects);
        }

        // split the socket into read and write halves
        let (read_half, mut write_half) = socket.into_split();

        // buf reader to read from the socket
        let mut read_stream = BufReader::new(read_half);

        // try and log into the APRS-IS server
        let login_ok;
        tokio::select! {

            _ = token.cancelled() => {
                break;
            },

            result = timeout(LOGIN_TIMEOUT, login_to_aprsis(&mut write_half, &mut read_stream, &loginstring, &host, &port, rw)) => {
                match result {
                    Ok(Ok(login)) => {
                        if login {
                            info!("Login to {}:{} successful", host, port);
                            login_ok = true;
                        }
                        else {
                            warn!("Login to {}:{} failed (unverified). Retrying in {}s...", host, port, backoff_secs);
                            login_ok = false;
                        }
                    },
                    Ok(Err(e)) => {
                        warn!("Error trying to log into {}:{}: {}. Retrying in {}s...", host, port, e, backoff_secs);
                        login_ok = false;
                    },
                    Err(_elapsed) => {
                        warn!("Login to {}:{} timed out after {}s. Retrying in {}s...", host, port, LOGIN_TIMEOUT.as_secs(), backoff_secs);
                        login_ok = false;
                    }
                }
            }
        }

        if !login_ok {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = sleep(Duration::from_secs(backoff_secs)) => {},
            }
            backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            continue;
        }

        // inner loop:  loop forever reading data from the channels and the aprs-is server
        loop {

            let mut raw = Vec::new();

            tokio::select! {

                _ = token.cancelled() => {
                    info!("Shutting down APRS-IS connection to {}:{}", host, port);
                    let _ = write_half.shutdown().await;
                    break;
                },

                // send statistics to the data channel
                _ = telemetry_interval.tick() => {

                    let the_time = Local::now();

                    // Purge expired dedup entries on a steady cadence so the cache
                    // doesn't accumulate stale rows during long idle stretches.
                    let now_ts = the_time.timestamp();
                    dedup_cache.retain(|_, ts| now_ts - *ts < DEDUP_TTL_SECS);

                    // add data points to series
                    rf_received_series.data.push_back(DataPoint { timestamp: the_time, value: stats_rf_received });
                    dropped_series.data.push_back(DataPoint { timestamp: the_time, value: packets_dropped });
                    igated_series.data.push_back(DataPoint { timestamp: the_time, value: packets_igated });
                    reconnect_series.data.push_back(DataPoint { timestamp: the_time, value: reconnects_this_interval });

                    // trim series to max 100 data points
                    for series in [&mut rf_received_series, &mut dropped_series, &mut igated_series, &mut reconnect_series] {
                        if series.data.len() > 100 {
                            series.data.pop_front();
                        }
                    }

                    let now = Utc::now();
                    let microsecs: f64 = now.timestamp_micros() as f64 / 1000000.0;

                    let data = AprsisTelemetry {
                        name: String::from("aprsis_statistics"),
                        timestamp: the_time,
                        microsecs,
                        reconnects: reconnect_series.clone(),
                        rf_received: rf_received_series.clone(),
                        packets_dropped: dropped_series.clone(),
                        packets_igated: igated_series.clone(),
                        lifetime_rf_received,
                        lifetime_packets_igated,
                        lifetime_packets_dropped,
                        lifetime_reconnects: total_reconnects as u64,
                        lifetime_drops_stale,
                        lifetime_drops_rfonly,
                        lifetime_drops_query,
                        lifetime_drops_thirdparty,
                        lifetime_drops_sat,
                        lifetime_drops_duplicate,
                        lifetime_drops_malformed,
                        lifetime_lagged_drops,
                    };

                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::AprsisStatus(data))) {
                        warn!("Failed to send statistics data to channel: {}", e);
                    }

                    // reset counters
                    packets_dropped = 0;
                    packets_igated = 0;
                    stats_rf_received = 0;
                    reconnects_this_interval = 0;
                },

                // read from APRS-IS server (consume data to detect EOF/errors)
                read_result = read_stream.read_until(b'\n', &mut raw) => {
                    match read_result {
                        Ok(0) => {
                            warn!("APRS-IS server {}:{} closed connection", host, port);
                            break;
                        },
                        Ok(_) => {
                            let s = String::from_utf8_lossy(&raw);
                            if s.starts_with('#') {
                                debug!("{}:{}: {}", host, port, s.trim_end());
                            }
                        },
                        Err(e) => {
                            warn!("Unable to read from {}:{}: {}", host, port, e);
                            break;
                        },
                    }
                },

                // check if there's any data to be read from the data channel
                message = data_stream.recv() => {
                    match message {
                        Ok(DataItem::Pkt(packet)) => {
                            match packet {
                                Packet::RTP(p) => {

                                    // update counters
                                    rf_received += 1;
                                    stats_rf_received += 1;
                                    lifetime_rf_received += 1;
                                    heard_direct += p.heard_direct as u32;
                                    received_sat += p.is_satellite as u32;
                                    if p.frequency != 144.390 {
                                        received_other += 1;
                                    };

                                    // if igating is enabled...
                                    if igating && rw {

                                        if let Some(reason) = igate::droppacket(&p) {
                                            warn!("dropping packet ({}): {}MHz rfonly: {} Direct: {}  {}", reason.as_str(), p.frequency, p.rfonly as u32, p.heard_direct as u32, p.raw);

                                            match reason {
                                                igate::DropReason::Stale => lifetime_drops_stale += 1,
                                                igate::DropReason::RfOnly => lifetime_drops_rfonly += 1,
                                                igate::DropReason::GenericQuery => lifetime_drops_query += 1,
                                                igate::DropReason::ThirdPartyInternet => lifetime_drops_thirdparty += 1,
                                                igate::DropReason::SatelliteDirect => lifetime_drops_sat += 1,
                                                igate::DropReason::MalformedField => lifetime_drops_malformed += 1,
                                            }

                                            dropped += 1;
                                            packets_dropped += 1;
                                            lifetime_packets_dropped += 1;
                                        }
                                        else {
                                            // duplicate suppression: check if we've recently igated this exact packet.
                                            // Expired entries are purged on the telemetry tick (every 15 s).
                                            let dedup_key = format!("{}:{}", p.source, p.info);
                                            let now_ts = Local::now().timestamp();

                                            if let Some(last_ts) = dedup_cache.get(&dedup_key) {
                                                if now_ts - last_ts < DEDUP_TTL_SECS {
                                                    debug!("Suppressing duplicate: {}", p.raw);
                                                    dropped += 1;
                                                    packets_dropped += 1;
                                                    lifetime_packets_dropped += 1;
                                                    lifetime_drops_duplicate += 1;
                                                    continue;
                                                }
                                            }

                                            // reform packet into a byte string suitable for xmitting to an
                                            // APRS-IS server. We build it byte-faithfully (raw info bytes,
                                            // not the lossy UTF-8 String) so a packet's original 8-bit
                                            // payload is gated unchanged rather than mangled into U+FFFD.
                                            let mut packet_bytes = p.for_rxigate_bytes(&callsign);

                                            // Defense in depth — droppacket() already rejects fields
                                            // containing embedded CR/LF, but enforce again at the write
                                            // boundary so a future code path that bypasses gating cannot
                                            // smuggle a forged APRS-IS line over our authenticated session.
                                            // Trailing CR/LF is tolerated to match droppacket().
                                            let trimmed = {
                                                let end = packet_bytes.iter()
                                                    .rposition(|&b| b != b'\r' && b != b'\n')
                                                    .map_or(0, |i| i + 1);
                                                &packet_bytes[..end]
                                            };
                                            if trimmed.iter().any(|&b| b == b'\r' || b == b'\n') {
                                                warn!("Refusing to write packet with embedded CR/LF: {}", p.raw);
                                                dropped += 1;
                                                packets_dropped += 1;
                                                lifetime_packets_dropped += 1;
                                                lifetime_drops_malformed += 1;
                                                continue;
                                            }

                                            info!("Igating:  {}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                                            // append the line terminator and write to the socket (bounded
                                            // timeout — a hung server can otherwise block this task
                                            // indefinitely while RF packets pile up and lag out of the
                                            // broadcast channel)
                                            packet_bytes.extend_from_slice(b"\r\n");
                                            match timeout(WRITE_TIMEOUT, write_half.write_all(&packet_bytes)).await {
                                                Ok(Ok(())) => {},
                                                Ok(Err(e)) => {
                                                    warn!("Write to APRS-IS failed: {}", e);
                                                    break;
                                                },
                                                Err(_elapsed) => {
                                                    warn!("Write to APRS-IS timed out after {}s, reconnecting", WRITE_TIMEOUT.as_secs());
                                                    break;
                                                },
                                            }

                                            // record this packet in the dedup cache
                                            dedup_cache.insert(dedup_key, now_ts);

                                            packets_igated += 1;
                                            lifetime_packets_igated += 1;
                                        }
                                    }

                                },
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("APRS-IS data channel lagged, skipped {} messages", n);
                            lifetime_lagged_drops = lifetime_lagged_drops.saturating_add(n);
                        },
                        _ => (),
                    }
                },

                // wake up periodically to transmit telemetry data to the APRS-IS server.
                _ = time_interval.tick() => {

                    // only sends data to APRS-IS if beaconing is configured and igating.
                    if beaconing && rw {

                        // Resolve the beacon position. For the config source this is
                        // always the static location; for the GPSD source it is the
                        // live fix, or None when there is no fresh fix (skip beacon).
                        let beacon_loc = match position_source {
                            PositionSource::Config => Some(location.clone()),
                            PositionSource::Gpsd => gpsd_beacon_location(&gps_state, location.alt),
                        };

                        match beacon_loc {
                            Some(loc) => {
                                if !send_position_beacon(&mut write_half, &loc, &callsign, &station_name, &symbol, &overlay, dao_mode).await {
                                    break;
                                }
                                if let (Some(la), Some(lo)) = (loc.lat, loc.lon) {
                                    last_beacon_pos = Some((la, lo));
                                }
                            },
                            None => {
                                debug!("No fresh GPS fix; skipping position beacon (telemetry still sent)");
                            },
                        }


                        debug!("Telemetry counters.  sequence: {}, recieved: {}, heard_direct: {}, dropped: {}, received_sat: {}, received_other: {}", sequence, rf_received, heard_direct, dropped, received_sat, received_other);

                        let mut dropped_pct = 0.0;
                        let mut direct_pct = 0.0;

                        if rf_received > 0 {
                            dropped_pct = 100.0 * (dropped as f64) / (rf_received as f64);
                            direct_pct = 100.0 * (heard_direct as f64) / (rf_received as f64);

                            if dropped_pct < 0.0 {
                                dropped_pct = 0.0;
                            }
                            if direct_pct < 0.0 {
                                direct_pct = 0.0;
                            }
                        }

                        // create analog items for each counter
                        let items = vec![
                            AnalogItem { label: format!("Rx_{}min", telemetry_threshold / 60), units: String::from("Pkts"), equation: APRSQuadratic::new(rf_received as f64) },
                            AnalogItem { label: format!("RxSat_{}min", telemetry_threshold / 60), units: String::from("Pkts"), equation: APRSQuadratic::new(received_sat as f64) },
                            AnalogItem { label: format!("%Drop_{}min", telemetry_threshold / 60), units: String::from("%"), equation: APRSQuadratic::new(dropped_pct) },
                            AnalogItem { label: format!("%Direct_{}min", telemetry_threshold / 60), units: String::from("%"), equation: APRSQuadratic::new(direct_pct) },
                            AnalogItem { label: format!("RxAltFreq_{}min", telemetry_threshold / 60), units: String::from("Pkts"), equation: APRSQuadratic::new(received_other as f64) }
                        ];

                        let telem = Telemetry {
                            telemetry: items,
                            name: String::from("Telemetry Report"),
                            sequence,
                        };

                        let packets = match telem.to_aprs(&callsign) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Error creating telemetry packets: {}. Skipping.", e);
                                continue;
                            },
                        };
                        debug!("{:?}", packets);

                        // transmit this telemetry to the APRS-IS server
                        let mut write_failed = false;
                        for info_string in &packets {
                            let packet_text = format!("{}>{},TCPIP*:{}", callsign, TOCALL, info_string);

                            info!("xmitting: {}", packet_text);
                            match timeout(WRITE_TIMEOUT, write_half.write_all(format!("{}\r\n", packet_text).as_bytes())).await {
                                Ok(Ok(())) => {},
                                Ok(Err(e)) => {
                                    warn!("Write to APRS-IS failed: {}", e);
                                    write_failed = true;
                                    break;
                                },
                                Err(_elapsed) => {
                                    warn!("Write to APRS-IS timed out after {}s, reconnecting", WRITE_TIMEOUT.as_secs());
                                    write_failed = true;
                                    break;
                                },
                            }
                        }
                        if write_failed {
                            break;
                        }

                        // increment the sequence number
                        sequence += 1;

                        // save the sequence to a file
                        sequence = igate::write_telemetry_seq(telemetry_file, sequence).await?;

                        // clear stats
                        rf_received = 0;
                        dropped = 0;
                        heard_direct = 0;
                        received_other = 0;
                        received_sat = 0;
                    }
                },

                // GPSD movement-triggered beacon. Fires no faster than
                // `min_beacon_secs`; beacons only when the live position has moved
                // beyond `move_threshold_deg` since the last beacon. Position only —
                // telemetry stays on the fixed interval above.
                _ = move_interval.tick(), if position_source == PositionSource::Gpsd && beaconing && rw => {

                    if let Some(loc) = gpsd_beacon_location(&gps_state, location.alt) {
                        let moved = match (last_beacon_pos, loc.lat, loc.lon) {
                            (Some((plat, plon)), Some(la), Some(lo)) =>
                                (la - plat).abs() > move_threshold_deg || (lo - plon).abs() > move_threshold_deg,
                            // no prior beacon yet, but we have a fresh fix — beacon now
                            (None, Some(_), Some(_)) => true,
                            _ => false,
                        };

                        if moved {
                            if !send_position_beacon(&mut write_half, &loc, &callsign, &station_name, &symbol, &overlay, dao_mode).await {
                                break;
                            }
                            if let (Some(la), Some(lo)) = (loc.lat, loc.lon) {
                                last_beacon_pos = Some((la, lo));
                            }
                        }
                    }
                }

            } // tokio::select

        } // inner loop

    } // outer loop

    info!("Task ended.");

    Ok(())
}


/// Using the open socket and bufreader stream, attempt to log into the APRS-IS server.
/// The `rw` parameter indicates whether we expect a verified (read-write) connection.
async fn login_to_aprsis(writer: &mut tcp::OwnedWriteHalf, reader: &mut BufReader<tcp::OwnedReadHalf>, loginstring: &str, host: &str, port: &u32, rw: bool) -> Result<bool, RtpigateError> {

    // read the version line from the aprsis server to ensure a proper connection
    let mut raw = String::new();

    match reader.read_line(&mut raw).await {
        Ok(0) => {
            return Err(RtpigateError::Network(format!("Connection closed by {}:{} before login", host, port)));
        },
        Ok(_numbytes) => {
            debug!("{}:{}: {}", host, port, raw.trim_end());

            // send the login string to the aprs-is server
            writer.write_all(loginstring.as_bytes()).await?;
            writer.flush().await?;

            // read the response from APRS-IS
            let mut r = String::new();

            match reader.read_line(&mut r).await {
                Ok(0) => {
                    return Err(RtpigateError::Network(format!("Connection closed by {}:{} during login", host, port)));
                },
                Ok(_n) => {
                    debug!("{}:{}: {}", host, port, r.trim_end());

                    // verify login response if we expect a read-write connection
                    let response_lower = r.to_lowercase();
                    if rw && response_lower.contains("unverified") {
                        warn!("APRS-IS login response indicates unverified: {}", r.trim_end());
                        return Ok(false);
                    }
                    if rw && !response_lower.contains("verified") {
                        warn!("APRS-IS login response missing verification: {}", r.trim_end());
                        return Ok(false);
                    }
                },

                Err(e) => {
                    return Err(RtpigateError::Network(format!("Error reading login response from {}:{}: {}", host, port, e)));
                }
            }
        },

        Err(e) => {
            return Err(RtpigateError::Network(format!("Error reading from {}:{}: {}", host, port, e)));
        }
    }

    Ok(true)
}
