use tokio::{net::UdpSocket, sync::broadcast, time::{interval, sleep, Duration}};
use tokio_util::sync::CancellationToken;
use std::{collections::{HashMap, VecDeque}, net::{Ipv4Addr, SocketAddr, ToSocketAddrs}, sync::Arc, fmt, time::Instant};
use rtp_rs::RtpReader;
use serde::Serialize;
use socket2::{Domain, Protocol, Socket, Type};
use chrono::{DateTime, Local, Utc};
use ax25::frame::{Ax25Frame, FrameContent};

use log::{info, warn, error, debug};
use aprs_parser::{AprsPacket, AprsData};

use crate::config::{Config, AppTelemetry, PacketTelemetry, DataSeries, DataPoint, DataItem, StationEntry, StationTelemetry, FrequencyCount};
use crate::error::RtpigateError;

// the packet structure (created by the RTP thread for incoming RTP packets)
#[derive(Debug, Clone, Serialize)]
pub struct RTPPacket {

    // when we initial received this packet over the network
    pub receivetime: DateTime<Local>,

    // monotonic clock reading at the same moment as `receivetime`. Used for
    // the staleness/age check so NTP corrections cannot spuriously age packets
    // out of the gating pipeline. Not serialised to SSE clients.
    #[serde(skip)]
    pub received_instant: Instant,

    // the packet itself 
    pub raw: String,
    pub info: String,

    // viapath
    pub path: String,

    // filtered digipeater path (excludes WIDE*, TCPIP, etc.)
    pub digipeater_path: Vec<String>,
    pub hops: u32,

    // APRS data type
    pub ptype: char,

    // source and destination
    pub source: String,
    pub destination: String,

    // was this packet heard directly or from a digipeater.
    //
    // `heard_direct` follows the conventional "ignore fill-in digis" semantics:
    // a path of only WIDE1-1* still reports as direct. This matches how most
    // APRS UIs label packets, but it is *not* a strict "no asterisks anywhere"
    // check — for that, use `was_digipeated`.
    pub heard_direct: bool,
    pub heardfrom: String,

    // strict "any digipeater touched this packet" flag — true iff *any* address
    // in the path has its has-been-repeated bit set, including WIDE-class fill-ins.
    // Used by the satellite igate filter so a packet relayed by an unnamed
    // fill-in digi is correctly recognised as having been digipeated.
    pub was_digipeated: bool,

    // is this packet not to be igated and sent to the APRS-IS cloud?
    pub rfonly: bool,

    // the frequency from the RTP packet usually from ssrc()
    pub frequency: f64,

    // was this packet heard from or perhaps, destined to a satellite?
    pub is_satellite: bool,

    // whether this packet would be igated by droppacket() at receive time.
    // Mirrors what aprs_is.rs will decide, minus the dedup step — duplicates
    // within the gating window are still counted as "would-igate" here.
    pub igated: bool,

    // object or item name (if this packet is an object/item report)
    pub object_name: Option<String>,

    // parsed position data (if available)
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,
}

impl fmt::Display for RTPPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{0: <24} {1: <10} {2:.3}MHz direct: {3: <6} rfonly: {4: <6} {5:}", 
            self.receivetime.format("%Y-%m-%d %H:%M:%S%.3f"), 
            self.source,
            self.frequency,
            self.heard_direct,
            self.rfonly,
            self.raw,
        )
    }
}

impl RTPPacket {
    pub fn for_rxigate(&self, callsign: &str) -> String {
        // Direct-heard packets have an empty viapath. Including the empty path
        // would produce a double comma (SRC>DST,,qAO,...), which APRS-IS parsers
        // treat inconsistently, so omit the path element entirely in that case.
        if self.path.is_empty() {
            format!("{}>{},qAO,{}:{}", self.source, self.destination, callsign, self.info)
        } else {
            format!("{}>{},{},qAO,{}:{}", self.source, self.destination, self.path, callsign, self.info)
        }
    }
}

// type of packets
#[derive(Debug, Clone)]
pub enum Packet {
    RTP(RTPPacket),
}


// Retuns a SocketAddr from the "hostname:port" string provided
fn get_multicast_socket_addr(address: &str) -> Result<SocketAddr, RtpigateError> {

    // convert the address to socket address
    let mut addrs = address.to_socket_addrs()
        .map_err(|e| RtpigateError::Network(format!("Unable to parse address {}: {}", address, e)))?;

    addrs.next()
        .ok_or_else(|| RtpigateError::Network(format!("No valid socket address found for {}", address)))
}


// return a new UDP socket bound to the provided multicast address
fn bind_multicast(multicast_addr: SocketAddr) -> Result<std::net::UdpSocket, RtpigateError> {

    if !multicast_addr.ip().is_multicast() {
        return Err(RtpigateError::Validation(format!("{} is not a multicast address", multicast_addr)));
    }

    // create a socket for connecting
    let std_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    std_socket.set_reuse_address(true)?;

    // Request a generous UDP receive buffer to absorb scheduler delays and bursts.
    // Linux silently caps this to net.core.rmem_max — log the actual size so the
    // operator can raise the sysctl if needed.
    const REQUESTED_RCVBUF: usize = 8 * 1024 * 1024;
    if let Err(e) = std_socket.set_recv_buffer_size(REQUESTED_RCVBUF) {
        warn!("Unable to set UDP recv buffer to {} bytes: {}", REQUESTED_RCVBUF, e);
    }
    match std_socket.recv_buffer_size() {
        Ok(actual) => {
            if actual < REQUESTED_RCVBUF {
                warn!(
                    "UDP recv buffer is {} bytes (requested {}). \
                     Increase net.core.rmem_max to reduce kernel-level packet drops.",
                    actual, REQUESTED_RCVBUF
                );
            } else {
                info!("UDP recv buffer: {} bytes", actual);
            }
        },
        Err(e) => warn!("Unable to read UDP recv buffer size: {}", e),
    }

    std_socket.bind(&multicast_addr.into())?;
    std_socket.set_nonblocking(true)?;

    if let SocketAddr::V4(addr_v4) = multicast_addr {
        let multi_addr = addr_v4.ip();
        std_socket.join_multicast_v4(multi_addr, &Ipv4Addr::UNSPECIFIED)?;
        Ok(std_socket.into())
    } else {
        Err(RtpigateError::Validation("Only IPv4 multicast is supported".into()))
    }
}


// Sets up a UDP socket bound to the given multicast address
fn setup_multicast_socket(address: &str) -> Result<UdpSocket, RtpigateError> {
    let multicast_addr = get_multicast_socket_addr(address)?;
    let std_socket = bind_multicast(multicast_addr)?;
    let udp_socket = UdpSocket::from_std(std_socket)?;
    Ok(udp_socket)
}


// Listens for RTP packets on a given multicast address and calls the parse_rtp_packet function
// with the parsed RTP header and payload.  Normally, this will never return - it loops forever.
pub async fn rtp_listener(
    data_channel: broadcast::Sender<DataItem>,
    token: CancellationToken,
    config: Arc<Config>,
    sat_packet_log: Arc<std::sync::RwLock<VecDeque<RTPPacket>>>,
) -> Result<(), RtpigateError> {

    info!("Started");

    // get the address and port of the multicast end point we need to connect too
    let address = format!("{}:{}", config.rtp.host, config.rtp.port);

    // satellite frequencies sourced from config (default [145.825] if unset)
    let sat_freqs = config.satellite_frequencies();

    // per-interval statistics
    let mut heard_direct = 0;
    let mut digipeated = 0;
    let mut decode_errors = 0;
    let mut total_packets = 0;

    // lifetime counters (never reset)
    let mut lifetime_total_packets: u64 = 0;
    let mut lifetime_heard_direct: u64 = 0;
    let mut lifetime_digipeated: u64 = 0;
    let mut lifetime_decode_errors: u64 = 0;

    // station tracking (never cleared)
    let mut station_map: HashMap<String, StationEntry> = HashMap::new();
    let mut freq_counts: HashMap<String, u64> = HashMap::new();

    // data series
    let mut packets_series = DataSeries {
        name: String::from("total_packets"),
        data: VecDeque::new(),
    };
    let mut heard_direct_series = DataSeries {
        name: String::from("heard_direct"),
        data: VecDeque::new(),
    };
    let mut digipeated_series = DataSeries {
        name: String::from("digipeated"),
        data: VecDeque::new(),
    };
    let mut decode_errors_series = DataSeries {
        name: String::from("decode_errors"),
        data: VecDeque::new(),
    };

    // the interval for when to send statistics
    let mut time_interval = interval(Duration::from_secs(15));

    // backoff state for reconnection
    let mut backoff_secs: u64 = 5;
    const MAX_BACKOFF_SECS: u64 = 300;

    // buffer where we store incoming bytes
    let mut buf = [0u8; 1500];

    // outer reconnection loop
    loop {

        if token.is_cancelled() {
            break;
        }

        // attempt socket setup
        let udp_socket = match setup_multicast_socket(&address) {
            Ok(s) => {
                backoff_secs = 5;
                s
            },
            Err(e) => {
                error!("RTP socket setup failed: {}. Retrying in {}s...", e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        info!("Connected to RTP multicast address: {}", address);

        // inner packet read loop
        loop {

            tokio::select! {

                // was this thread canceled?
                _ = token.cancelled() => {
                    break;
                },


                // send statistics
                _ = time_interval.tick() => {
                    let the_time = Local::now();

                    // add data points to series
                    packets_series.data.push_back(DataPoint { timestamp: the_time, value: total_packets, });
                    heard_direct_series.data.push_back(DataPoint { timestamp: the_time, value: heard_direct, });
                    digipeated_series.data.push_back(DataPoint { timestamp: the_time, value: digipeated, });
                    decode_errors_series.data.push_back(DataPoint { timestamp: the_time, value: decode_errors, });

                    // trim series to max 100 data points
                    for series in [&mut packets_series, &mut heard_direct_series, &mut digipeated_series, &mut decode_errors_series] {
                        if series.data.len() > 100 {
                            series.data.pop_front();
                        }
                    }

                    // get the current time
                    let now = Utc::now();

                    // and in microsecs since the epoch
                    let microsecs: f64 = now.timestamp_micros() as f64 / 1000000.0;

                    // create a new struct to hold data statistics
                    let data = PacketTelemetry {
                        name: String::from("packet_statistics"),
                        timestamp: the_time,
                        microsecs,
                        total_packets: packets_series.clone(),
                        heard_direct: heard_direct_series.clone(),
                        digipeated: digipeated_series.clone(),
                        decode_errors: decode_errors_series.clone(),
                        lifetime_total_packets,
                        lifetime_heard_direct,
                        lifetime_digipeated,
                        lifetime_decode_errors,
                    };

                    // Send statistics to the channel
                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::PacketStatus(data))) {
                        warn!("Failed to send statistics data to channel: {}", e);
                    }

                    // Evict stations not heard in the last 36 hours
                    let evict_threshold = chrono::Duration::hours(36);
                    let now = Local::now();
                    station_map.retain(|_, entry| now - entry.last_heard < evict_threshold);

                    // Prune the satellite packet log of entries older than 24 hours.
                    let sat_log_threshold = chrono::Duration::hours(24);
                    if let Ok(mut log) = sat_packet_log.write() {
                        while let Some(front) = log.front() {
                            if now - front.receivetime > sat_log_threshold {
                                log.pop_front();
                            } else {
                                break;
                            }
                        }
                    }
                    freq_counts.retain(|freq, _| {
                        station_map.values().any(|e| format!("{:.3}", e.frequency) == *freq)
                    });

                    // Emit station statistics
                    let mut stations: Vec<StationEntry> = station_map.values().cloned().collect();
                    stations.sort_by(|a, b| b.count.cmp(&a.count));

                    let mut frequencies: Vec<FrequencyCount> = freq_counts.iter()
                        .map(|(f, c)| FrequencyCount { frequency: f.clone(), count: *c })
                        .collect();
                    frequencies.sort_by(|a, b| b.count.cmp(&a.count));

                    let station_data = StationTelemetry {
                        name: String::from("station_statistics"),
                        stations,
                        frequencies,
                    };

                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::StationStatus(station_data))) {
                        warn!("Failed to send station statistics to channel: {}", e);
                    }

                    // reset per-interval counters
                    heard_direct = 0;
                    digipeated = 0;
                    decode_errors = 0;
                    total_packets = 0;

                },


                // read from the socket
                result = udp_socket.recv(&mut buf) => {
                    match result {

                        // read was successful
                        Ok(num_bytes) => {
                            match RtpReader::new(&buf[..num_bytes]) {
                                Ok(rtp) => {

                                    // decode this packet into APRS then add it to downstream consumer queues
                                    // (i.e. aprs-is connection for igating, database writes, etc.)
                                    match parse_rtp_packet(rtp) {
                                        Ok(mut p) => {

                                            // Apply runtime-config-driven flags. is_satellite must be
                                            // set before droppacket() so the sat-frequency policy fires.
                                            p.is_satellite = sat_freqs.contains(&p.frequency);
                                            p.igated = crate::igate::droppacket(&p).is_none();

                                            if p.heard_direct {
                                                heard_direct += 1;
                                                lifetime_heard_direct += 1;
                                            } else {
                                                digipeated += 1;
                                                lifetime_digipeated += 1;
                                            }
                                            total_packets += 1;
                                            lifetime_total_packets += 1;

                                            // update station tracking
                                            let freq_key = format!("{:.3}", p.frequency);
                                            *freq_counts.entry(freq_key).or_insert(0) += 1;

                                            // extract symbol table/code from info field
                                            let (sym_table, sym_code) = extract_symbol_chars(&p.info, &p.destination);

                                            // use object/item name as station key if present
                                            let station_key = p.object_name.clone().unwrap_or_else(|| p.source.clone());
                                            let transmitted_by = p.object_name.as_ref().map(|_| p.source.clone());

                                            let entry = station_map.entry(station_key.clone()).or_insert_with(|| StationEntry {
                                                callsign: station_key,
                                                transmitted_by: transmitted_by.clone(),
                                                last_heard: p.receivetime,
                                                frequency: p.frequency,
                                                latitude: None,
                                                longitude: None,
                                                altitude_ft: None,
                                                heard_direct: p.heard_direct,
                                                position_path: p.digipeater_path.clone(),
                                                position_hops: p.hops,
                                                altitude_path: p.digipeater_path.clone(),
                                                altitude_hops: p.hops,
                                                symbol_table: None,
                                                symbol_code: None,
                                                count: 0,
                                            });
                                            entry.last_heard = p.receivetime;
                                            entry.frequency = p.frequency;
                                            entry.heard_direct = p.heard_direct;
                                            if let Some(ref tb) = transmitted_by {
                                                entry.transmitted_by = Some(tb.clone());
                                            }
                                            entry.count += 1;
                                            if p.latitude.is_some() && p.longitude.is_some() {
                                                entry.latitude = p.latitude;
                                                entry.longitude = p.longitude;
                                                entry.position_path = p.digipeater_path.clone();
                                                entry.position_hops = p.hops;
                                            }
                                            if let Some(alt) = p.altitude_ft {
                                                if entry.altitude_ft.is_none_or(|prev| alt > prev) {
                                                    entry.altitude_path = p.digipeater_path.clone();
                                                    entry.altitude_hops = p.hops;
                                                }
                                                entry.altitude_ft = Some(entry.altitude_ft.map_or(alt, |prev| prev.max(alt)));
                                            }
                                            if let Some(st) = sym_table {
                                                entry.symbol_table = Some(st);
                                            }
                                            if let Some(sc) = sym_code {
                                                entry.symbol_code = Some(sc);
                                            }

                                            // log this
                                            debug!("{:3.3}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                                            // append to the 24h satellite packet log (newest-first
                                            // ordering is maintained at read time).
                                            if p.is_satellite {
                                                if let Ok(mut log) = sat_packet_log.write() {
                                                    log.push_back(p.clone());
                                                }
                                            }

                                            // attempt to send this packet to the channel so downstream
                                            // consumers can process this packet.
                                            if let Err(e) = data_channel.send(DataItem::Pkt(Packet::RTP(p))) {
                                                warn!("Channel send failed: {}", e);
                                            }
                                        },

                                        Err(e) => {
                                            decode_errors += 1;
                                            lifetime_decode_errors += 1;
                                            warn!("RTP parse error: {}", e);
                                        },
                                    };
                                },

                                Err(e) => warn!("Not an RTP packet: {:?}", e),
                            }
                        },

                        // socket read error - break inner loop to reconnect
                        Err(e) => {
                            error!("RTP socket read failed: {}. Will reconnect...", e);
                            break;
                        },
                    }
                },
            }
        } // inner loop
    } // outer reconnection loop

    // drop the channel
    drop(data_channel);

    info!("Task ended.");

    Ok(())
}


// Constant slices for packet classification — no heap allocation per packet
const EXCLUDED_ADDRS: &[&str] = &["WIDE", "TCPIP", "NOGATE", "RFONLY", "SGATE"];
const RFONLY_ADDRS: &[&str] = &["TCPIP", "TCPXX", "RFONLY", "NOGATE"];

// used to parse an incoming RTP packet (w/ AX25 payload) into various source, destination,
// addresses, info fields.
fn parse_rtp_packet(rtp: RtpReader) -> Result<RTPPacket, RtpigateError> {

    // Attempt to parse the payload
    match Ax25Frame::from_bytes(rtp.payload()) {
        Ok(ax25_frame) => {

            // the time this packet was received. Capture both the wall-clock
            // (for display/SSE) and a monotonic Instant (for age checks).
            let receivetime = Local::now();
            let received_instant = Instant::now();

            // the frequency this packet was heard over
            let frequency = rtp.ssrc() as f64 / 1000.0;

            // get the ax25 frame from the rtp packet's payload
            let ax25infofield = match ax25_frame.content {
                FrameContent::UnnumberedInformation(information) => information.info,
                _ => return Err(RtpigateError::Parse(format!("{}MHz Not an AX.25 UI frame", frequency))),
            };

            let source = ax25_frame.source.to_string();
            let destination = ax25_frame.destination.to_string();

            // stringify route elements once and reuse
            let route_strings: Vec<(String, bool)> = ax25_frame.route.iter()
                .map(|p| (p.repeater.to_string(), p.has_repeated))
                .collect();

            // construct the viapath
            let viapath = route_strings.iter()
                .map(|(s, _)| s.as_str())
                .collect::<Vec<&str>>()
                .join(",");

            // filtered digipeater path (real callsigns only, no WIDE/TCPIP/etc.)
            let digipeater_path: Vec<String> = route_strings.iter()
                .filter(|(s, _)| EXCLUDED_ADDRS.iter().all(|x| !s.contains(x)))
                .map(|(s, _)| s.clone())
                .collect();
            let hops = digipeater_path.len() as u32;

            // build the APRS text efficiently using push_str
            let mut aprstext = String::with_capacity(source.len() + destination.len() + viapath.len() + 64);
            aprstext.push_str(&source);
            aprstext.push('>');
            aprstext.push_str(&destination);
            aprstext.push(',');
            aprstext.push_str(&viapath);

            // did this station hear this packet directly or was it digipeated?
            let heard_direct: bool = !route_strings.iter()
                .any(|(s, repeated)| *repeated && EXCLUDED_ADDRS.iter().all(|x| !s.contains(x)));

            // strict variant: any address at all that has been repeated, including WIDE.
            // Used by the satellite igate filter — see igate::droppacket.
            let was_digipeated: bool = route_strings.iter().any(|(_, repeated)| *repeated);

            let heardfrom: String = if heard_direct {
                source.clone()
            } else {
                route_strings.iter()
                    .filter(|(s, repeated)| *repeated && EXCLUDED_ADDRS.iter().all(|x| !s.contains(x)))
                    .last()
                    .map(|(s, _)| s.clone())
                    .unwrap_or_else(|| source.clone())
            };

            // Check if this packet is RF only and should not be igated
            let rfonly: bool = route_strings.iter()
                .any(|(s, _)| RFONLY_ADDRS.iter().any(|x| s.contains(x)));

            // the info data field
            let mut infodata: Vec<u8> = ax25infofield;

            // if there isn't an information field then we don't have a valid packet
            if infodata.len() >= 2 {
                // truncate off the last two bytes as those are the FCS crc data for the AX25 frame
                infodata.truncate(infodata.len()-2);
            } else {
                return Err(RtpigateError::Parse(format!("{}MHz AX.25 frame missing information field: {}", frequency, aprstext)));
            }

            // convert the information field for the APRS packet to UTF8 text
            let info = match String::from_utf8(infodata) {
                Ok(s) => s,
                Err(e) => {
                    let lossy_info_field = String::from_utf8_lossy(e.as_bytes());
                    return Err(RtpigateError::Parse(format!("{}MHz UTF-8 conversion failed: {}:{}. {}", frequency, aprstext, lossy_info_field, e)));
                },
            };

            // append the information field
            aprstext.push(':');
            aprstext.push_str(info.trim());

            // the APRS packet type
            let ptype: char = match info.chars().next() {
                Some(c) => c,
                None => return Err(RtpigateError::Parse("No APRS data type found".into())),
            };

            // attempt to parse position data using aprs-parser-rs
            let (mut latitude, mut longitude, mut altitude_ft) = (None, None, None);
            let mut object_name: Option<String> = None;
            if let Ok(parsed) = AprsPacket::decode_textual(aprstext.as_bytes()) {
                match parsed.data {
                    AprsData::Position(pos) => {
                        latitude = Some(*pos.position.latitude);
                        longitude = Some(*pos.position.longitude);
                        // check for altitude in comment (/A=NNNNNN)
                        let comment = String::from_utf8_lossy(&pos.comment);
                        if let Some(alt_idx) = comment.find("/A=") {
                            let alt_str = &comment[alt_idx + 3..];
                            let end = alt_str.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(alt_str.len());
                            if let Ok(alt) = alt_str[..end].parse::<f64>() {
                                altitude_ft = Some(alt);
                            }
                        }
                    },
                    AprsData::MicE(mice) => {
                        latitude = Some(*mice.latitude);
                        longitude = Some(*mice.longitude);
                        if let Some(alt) = mice.altitude {
                            altitude_ft = Some(alt.altitude_feet());
                        }
                    },
                    AprsData::Object(obj) => {
                        object_name = Some(String::from_utf8_lossy(&obj.name).to_string());
                        latitude = Some(*obj.position.latitude);
                        longitude = Some(*obj.position.longitude);
                        let comment = String::from_utf8_lossy(&obj.comment);
                        if let Some(alt_idx) = comment.find("/A=") {
                            let alt_str = &comment[alt_idx + 3..];
                            let end = alt_str.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(alt_str.len());
                            if let Ok(alt) = alt_str[..end].parse::<f64>() {
                                altitude_ft = Some(alt);
                            }
                        }
                    },
                    AprsData::Item(item) => {
                        object_name = Some(String::from_utf8_lossy(&item.name).to_string());
                        latitude = Some(*item.position.latitude);
                        longitude = Some(*item.position.longitude);
                        let comment = String::from_utf8_lossy(&item.comment);
                        if let Some(alt_idx) = comment.find("/A=") {
                            let alt_str = &comment[alt_idx + 3..];
                            let end = alt_str.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(alt_str.len());
                            if let Ok(alt) = alt_str[..end].parse::<f64>() {
                                altitude_ft = Some(alt);
                            }
                        }
                    },
                    _ => {},
                }
            }

            // return a new Packet structure. is_satellite and igated are set
            // by rtp_listener after parsing, since they depend on runtime config.
            Ok(RTPPacket {
                receivetime,
                received_instant,
                is_satellite: false,
                igated: false,
                frequency,
                path: viapath,
                digipeater_path,
                hops,
                heardfrom,
                heard_direct,
                was_digipeated,
                rfonly,
                ptype,
                source,
                destination,
                raw: aprstext,
                info,
                object_name,
                latitude,
                longitude,
                altitude_ft,
            })
        },
        Err(e) => Err(RtpigateError::Parse(format!("AX.25 frame parse failed: {:?}", e))),
    }
}

/// Extract APRS symbol table and code characters from the info field.
/// Returns (Option<symbol_table>, Option<symbol_code>).
fn extract_symbol_chars(info: &str, _destination: &str) -> (Option<char>, Option<char>) {
    if info.len() < 2 {
        return (None, None);
    }
    let data_type = info.as_bytes()[0] as char;
    let chars: Vec<char> = info.chars().collect();

    match data_type {
        '!' | '=' => {
            if chars.len() >= 2 {
                let c1 = chars[1];
                if c1.is_ascii_digit() {
                    // uncompressed: !DDMM.MMN<table>DDDMM.MMW<code>
                    if chars.len() >= 20 {
                        return (Some(chars[9]), Some(chars[19]));
                    }
                } else {
                    // compressed: !<table>YYYYXXXX<code>csT
                    if chars.len() >= 11 {
                        return (Some(c1), Some(chars[10]));
                    }
                }
            }
        },
        '/' | '@' => {
            // timestamped: skip 7-char timestamp + data_type = offset 8
            if chars.len() >= 9 {
                let c8 = chars[8];
                if c8.is_ascii_digit() {
                    if chars.len() >= 27 {
                        return (Some(chars[16]), Some(chars[26]));
                    }
                } else {
                    if chars.len() >= 18 {
                        return (Some(c8), Some(chars[17]));
                    }
                }
            }
        },
        '`' | '\'' => {
            // Mic-E: symbol at chars[7], table at chars[8]
            if chars.len() >= 9 {
                return (Some(chars[8]), Some(chars[7]));
            }
        },
        ';' => {
            // Object: ;name(9)*timestamp(7)position...
            // offset 0=';', 1-9=name, 10=live/dead, 11-17=timestamp, 18+=position
            if chars.len() >= 19 {
                let c18 = chars[18];
                if c18.is_ascii_digit() {
                    // uncompressed: table at offset 26, code at offset 36
                    if chars.len() >= 37 {
                        return (Some(chars[26]), Some(chars[36]));
                    }
                } else {
                    // compressed: table at offset 18, code at offset 27
                    if chars.len() >= 28 {
                        return (Some(c18), Some(chars[27]));
                    }
                }
            }
        },
        ')' => {
            // Item: )name(3-9)live/dead position...
            // find the live/dead marker ('!' or ' ') to locate position start
            let name_end = chars[1..].iter().take(9).position(|&c| c == '!' || c == ' ');
            if let Some(ne) = name_end {
                let pos_start = 1 + ne + 1; // skip ')' + name + live/dead marker
                if chars.len() > pos_start {
                    let c = chars[pos_start];
                    if c.is_ascii_digit() {
                        // uncompressed: table at pos_start+8, code at pos_start+18
                        if chars.len() >= pos_start + 19 {
                            return (Some(chars[pos_start + 8]), Some(chars[pos_start + 18]));
                        }
                    } else {
                        // compressed: table at pos_start, code at pos_start+9
                        if chars.len() >= pos_start + 10 {
                            return (Some(c), Some(chars[pos_start + 9]));
                        }
                    }
                }
            }
        },
        _ => {},
    }
    (None, None)
}

