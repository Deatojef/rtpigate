use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{tcp, TcpStream}, sync::broadcast, time::{interval, Duration}};
use tokio_util::sync::CancellationToken;
use std::{collections::{HashMap, VecDeque}, str, sync::Arc};
use chrono::{Local, Utc};

use log::{info, warn, debug};
use tokio::time::sleep;

use crate::config::{Config, APRSISLogin, APRSISPasscode, AppTelemetry, DataSeries, DataPoint, AprsisTelemetry, DataItem};
use crate::error::RtpigateError;
use crate::ka9q::{InetPacket, Packet};
use crate::igate::{self, TOCALL, AnalogItem, APRSQuadratic, Telemetry};


/// Main APRS-IS task: manages the TCP connection and coordinates igating, beaconing, and telemetry.
pub async fn aprsis_task(data_channel: broadcast::Sender<DataItem>, token: CancellationToken, config: Arc<Config>) -> Result<(), RtpigateError> {

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
    let mut stats_inet_received: u32 = 0;

    // data series
    let mut rf_received_series = DataSeries { name: String::from("rf_received"), data: VecDeque::new() };
    let mut inet_received_series = DataSeries { name: String::from("inet_received"), data: VecDeque::new() };
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

    // station location
    let location = config.location.clone();

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

        // create a TCP stream
        let socket: TcpStream = match TcpStream::connect(sock_addr).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to connect to {}:{}: {}. Retrying in {}s...", host, port, e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

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

            result = login_to_aprsis(&mut write_half, &mut read_stream, &loginstring, &host, &port, rw) => {
                match result {
                    Ok(login) => {
                        if login {
                            info!("Login to {}:{} successful", host, port);
                            login_ok = true;
                        }
                        else {
                            warn!("Login to {}:{} failed (unverified). Retrying in {}s...", host, port, backoff_secs);
                            login_ok = false;
                        }
                    },
                    Err(e) => {
                        warn!("Error trying to log into {}:{}: {}. Retrying in {}s...", host, port, e, backoff_secs);
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

                    // add data points to series
                    rf_received_series.data.push_back(DataPoint { timestamp: the_time, value: stats_rf_received });
                    inet_received_series.data.push_back(DataPoint { timestamp: the_time, value: stats_inet_received });
                    dropped_series.data.push_back(DataPoint { timestamp: the_time, value: packets_dropped });
                    igated_series.data.push_back(DataPoint { timestamp: the_time, value: packets_igated });
                    reconnect_series.data.push_back(DataPoint { timestamp: the_time, value: reconnects_this_interval });

                    // trim series to max 100 data points
                    for series in [&mut rf_received_series, &mut inet_received_series, &mut dropped_series, &mut igated_series, &mut reconnect_series] {
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
                        inet_received: inet_received_series.clone(),
                        packets_dropped: dropped_series.clone(),
                        packets_igated: igated_series.clone(),
                    };

                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::AprsisStatus(data))) {
                        warn!("Failed to send statistics data to channel: {}", e);
                    }

                    // reset counters
                    packets_dropped = 0;
                    packets_igated = 0;
                    stats_rf_received = 0;
                    stats_inet_received = 0;
                    reconnects_this_interval = 0;
                },

                // check if there's anything we can read from the APRS-IS server
                read_result = read_stream.read_until(b'\n', &mut raw) => {
                    match read_result {
                        Ok(0) => {
                            warn!("APRS-IS server {}:{} closed connection", host, port);
                            break;
                        },
                        Ok(_numbytes) => {

                            let s = String::from_utf8_lossy(&raw);
                            match s.chars().next() {
                                Some(x) => match x {
                                    '#' => debug!("{}:{}: {}", host, port, s.trim_end()),
                                    _ => {

                                            debug!("trying to process packet from aprsis: {}", &s);

                                            if let Ok(p) = str::from_utf8(&raw) {

                                                let packet_text = p.trim();

                                                if let Some(source_delim) = packet_text.find(">") {

                                                    if let Some(info_delim) = packet_text.find(":") {

                                                        let source = &packet_text[0..source_delim];
                                                        let info = &packet_text[info_delim..];

                                                        if source.len() > 0 && info.len() > 0 {

                                                            if let Some(ptype) = info.chars().next() {
                                                                let packet = InetPacket {
                                                                    receivetime: Local::now(),
                                                                    info: info.to_owned(),
                                                                    source: source.to_owned(),
                                                                    ptype: ptype as char,
                                                                    raw: packet_text.to_owned(),
                                                                    aprsaddress: address.clone(),
                                                                };

                                                                match data_channel.send(DataItem::Pkt(Packet::Inet(packet))) {
                                                                    Ok(_a) => {
                                                                        debug!("---.---MHz Direct: 0  {}", packet_text);
                                                                        stats_inet_received += 1;
                                                                    },
                                                                    Err(e) => {
                                                                        warn!("Channel send failed: {}", e);
                                                                    },
                                                                };
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                    },
                                },
                                None => (),
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
                                    heard_direct += p.heard_direct as u32;
                                    received_sat += p.is_satellite as u32;
                                    if p.frequency != 144.390 {
                                        received_other += 1;
                                    };

                                    // if igating is enabled...
                                    if igating && rw {

                                        if igate::droppacket(&p) {
                                            warn!("dropping packet: {}MHz rfonly: {} Direct: {}  {}", p.frequency, p.rfonly as u32, p.heard_direct as u32, p.raw);

                                            dropped += 1;
                                            packets_dropped += 1;
                                        }
                                        else {
                                            // duplicate suppression: check if we've recently igated this exact packet
                                            let dedup_key = format!("{}:{}", p.source, p.info);
                                            let now_ts = Local::now().timestamp();

                                            // purge expired entries periodically (when cache grows)
                                            if dedup_cache.len() > 500 {
                                                dedup_cache.retain(|_, ts| now_ts - *ts < DEDUP_TTL_SECS);
                                            }

                                            if let Some(last_ts) = dedup_cache.get(&dedup_key) {
                                                if now_ts - last_ts < DEDUP_TTL_SECS {
                                                    debug!("Suppressing duplicate: {}", p.raw);
                                                    dropped += 1;
                                                    packets_dropped += 1;
                                                    continue;
                                                }
                                            }

                                            // reform packet into a string suitable for xmitting to an APRS-IS server
                                            let packet_text = p.for_rxigate(&callsign);

                                            info!("Igating:  {}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                                            // write data to the socket
                                            if let Err(e) = write_half.write_all(format!("{}\r\n", packet_text).as_bytes()).await {
                                                warn!("Write to APRS-IS failed: {}", e);
                                                break;
                                            }

                                            // record this packet in the dedup cache
                                            dedup_cache.insert(dedup_key, now_ts);

                                            packets_igated += 1;
                                        }
                                    }

                                },
                                _ => (),
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("APRS-IS data channel lagged, skipped {} messages", n);
                        },
                        _ => (),
                    }
                },

                // wake up periodically to transmit telemetry data to the APRS-IS server.
                _ = time_interval.tick() => {

                    // only sends data to APRS-IS if beaconing is configured and igating.
                    if beaconing && rw {

                        // create a posit packet for sending to the APRS-IS server
                        let posit_text = match igate::positpacket(&location, &callsign, &station_name, &symbol, &overlay) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Error creating position packet: {}. Skipping beacon.", e);
                                continue;
                            },
                        };

                        // transmit this position packet to the aprsis server.
                        info!("xmitting: {}", posit_text);
                        if let Err(e) = write_half.write_all(format!("{}\r\n", posit_text).as_bytes()).await {
                            warn!("Write to APRS-IS failed: {}", e);
                            break;
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
                            if let Err(e) = write_half.write_all(format!("{}\r\n", packet_text).as_bytes()).await {
                                warn!("Write to APRS-IS failed: {}", e);
                                write_failed = true;
                                break;
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
