use tokio::{net::UdpSocket, sync::broadcast, time::{interval, sleep, Duration}};
use tokio_util::sync::CancellationToken;
use std::{collections::VecDeque, io::{self, ErrorKind}, net::{Ipv4Addr, SocketAddr, ToSocketAddrs}, sync::Arc, error::Error, fmt};
use rtp_rs::RtpReader;
use serde::Serialize;
use socket2::{Domain, Protocol, Socket, Type};
use chrono::{DateTime, Local, Utc};
use ax25::frame::{Ax25Frame, FrameContent};

use log::{info, warn, error, debug};

use crate::config::{Config, AppTelemetry, PacketTelemetry, DataSeries, DataPoint, DataItem};

// the packet structure (created by the RTP thread for incoming RTP packets)
#[derive(Debug, Clone, Serialize)]
pub struct RTPPacket {

    // when we initial received this packet over the network
    pub receivetime: DateTime<Local>,

    // the packet itself 
    pub raw: String,
    pub info: String,

    // viapath
    pub path: String,

    // APRS data type
    pub ptype: char,

    // source and destination
    pub source: String,
    pub destination: String,

    // was this packet heard directly or from a digipeater
    pub heard_direct: bool,
    pub heardfrom: String,

    // is this packet not to be igated and sent to the APRS-IS cloud?
    pub rfonly: bool,

    // the frequency from the RTP packet usually from ssrc()
    pub frequency: f64,

    // was this packet heard from or perhaps, destined to a satellite?
    pub is_satellite: bool,
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
        format!(
            "{}>{},{},qAO,{}:{}",
            self.source,
            self.destination,
            self.path,
            callsign,
            self.info
        )
    }
}

// the packet structure for internet source packets (i.e. from APRS-IS)
#[derive(Debug, Clone, Serialize)]
pub struct InetPacket {
    // when we initial received this packet over the network
    pub receivetime: DateTime<Local>,

    // the packet itself 
    pub raw: String,
    pub info: String,

    // APRS data type
    pub ptype: char,

    // source and destination
    pub source: String,

    // the address of the APRS-IS server
    pub aprsaddress: String,
}

impl fmt::Display for InetPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{0: <24} {1:}",
            self.receivetime.format("%Y-%m-%d %H:%M:%S%.3f"), 
            self.raw,
        )
    }
}


// type of packets
#[derive(Debug, Clone)]
pub enum Packet {
    RTP(RTPPacket),
    Inet(InetPacket),
}


// Retuns a SocketAddr from the "hostname:port" string provided
fn get_multicast_socket_addr(address: &str) -> Result<SocketAddr, Box<dyn Error>> {

    // convert the address to socket address
    let mut addrs = match address.to_socket_addrs() {
        Ok(a) => a,
        Err(e) => return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to parse address, {} - {}", address, e)))),
    };

    match addrs.next() {
        Some(socketaddr) => Ok(socketaddr),
        None => Err(Box::new(io::Error::new(ErrorKind::Other, format!("No valid socket address found for, {}.", address))))
    }
}


// return a new UDP socket bound to the provided multicast address
fn bind_multicast(multicast_addr: SocketAddr) -> Result<std::net::UdpSocket, Box<dyn Error>> {
    
    if !multicast_addr.ip().is_multicast() {
        return Err(Box::new(io::Error::new(ErrorKind::AddrNotAvailable, "Address is not a multicast address")));
    }

    // create a socket for connecting
    let std_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    std_socket.set_reuse_address(true)?;
    std_socket.bind(&multicast_addr.into())?;
    std_socket.set_nonblocking(true)?;

    if let SocketAddr::V4(addr_v4) = multicast_addr {
        let multi_addr = addr_v4.ip();
        std_socket.join_multicast_v4(multi_addr, &Ipv4Addr::UNSPECIFIED)?;
        Ok(std_socket.into())
    } else {
        Err(Box::new(io::Error::new( ErrorKind::AddrNotAvailable, "Only IPv4 multicast is supported in this example")))
    }
}


// Sets up a UDP socket bound to the given multicast address
fn setup_multicast_socket(address: &str) -> Result<UdpSocket, Box<dyn Error + Send + Sync>> {
    let multicast_addr = get_multicast_socket_addr(address).map_err(|e| -> Box<dyn Error + Send + Sync> { e.to_string().into() })?;
    let std_socket = bind_multicast(multicast_addr).map_err(|e| -> Box<dyn Error + Send + Sync> { e.to_string().into() })?;
    let udp_socket = UdpSocket::from_std(std_socket).map_err(|e| -> Box<dyn Error + Send + Sync> { e.to_string().into() })?;
    Ok(udp_socket)
}


// Listens for RTP packets on a given multicast address and calls the parse_rtp_packet function
// with the parsed RTP header and payload.  Normally, this will never return - it loops forever.
pub async fn rtp_listener(data_channel: broadcast::Sender<DataItem>, token: CancellationToken, config: Arc<Config>) -> Result<(), Box<dyn Error>> {

    info!("Started");

    // get the address and port of the multicast end point we need to connect too
    let address = format!("{}:{}", config.rtp.host, config.rtp.port);

    // statistics
    let mut heard_direct = 0;
    let mut decode_errors = 0;
    let mut total_packets = 0;

    // data series
    let mut packets_series = DataSeries {
        name: String::from("total_packets"),
        data: VecDeque::new(),
    };
    let mut heard_direct_series = DataSeries {
        name: String::from("heard_direct"),
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
                    decode_errors_series.data.push_back(DataPoint { timestamp: the_time, value: decode_errors, });

                    // trim series to max 100 data points
                    if packets_series.data.len() > 100 {
                        packets_series.data.pop_front();
                    }
                    if heard_direct_series.data.len() > 100 {
                        heard_direct_series.data.pop_front();
                    }
                    if decode_errors_series.data.len() > 100 {
                        decode_errors_series.data.pop_front();
                    }

                    // get the current time
                    let now = Utc::now();

                    // and in microsecs since the epoch
                    let microsecs: f64 = now.timestamp_micros() as f64 / 1000000.0;

                    // create a new struct to hold data statistics
                    let data = PacketTelemetry {
                        name: String::from("packet_statistics"),
                        timestamp: the_time,
                        microsecs: microsecs,
                        total_packets: packets_series.clone(),
                        decode_errors: decode_errors_series.clone(),
                        heard_direct: heard_direct_series.clone(),
                    };

                    // Send statistics to the channel
                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::PacketStatus(data))) {
                        warn!("Failed to send statistics data to channel: {}", e);
                    }

                    // reset counters
                    heard_direct = 0;
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
                                        Ok(p) => {

                                            if p.heard_direct {
                                                heard_direct += 1;
                                            }
                                            total_packets += 1;

                                            // log this
                                            debug!("{:3.3}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                                            // attempt to send this packet to the channel so downstream
                                            // consumers can process this packet.
                                            if let Err(e) = data_channel.send(DataItem::Pkt(Packet::RTP(p))) {
                                                warn!("Channel send failed: {}", e);
                                            }
                                        },

                                        Err(e) => {
                                            decode_errors += 1;
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
const RFONLY_ADDRS: &[&str] = &["TCPIP", "RFONLY", "NOGATE"];
const SAT_FREQS: &[f64] = &[145.825];

// used to parse an incoming RTP packet (w/ AX25 payload) into various source, destination,
// addresses, info fields.
fn parse_rtp_packet(rtp: RtpReader) -> Result<RTPPacket, Box<dyn Error>> {

    // Attempt to parse the payload
    match Ax25Frame::from_bytes(rtp.payload()) {
        Ok(ax25_frame) => {

            // the time this packet was received.
            let receivetime = Local::now();

            // the frequency this packet was heard over
            let frequency = rtp.ssrc() as f64 / 1000.0;

            // get the ax25 frame from the rtp packet's payload
            let ax25infofield = match ax25_frame.content {
                FrameContent::UnnumberedInformation(information) => information.info,
                _ => return Err(Box::new(io::Error::new(ErrorKind::Other, format!("{}MHz Not an AX.25 UI frame.", frequency)))),
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
                return Err(Box::new(io::Error::new(ErrorKind::Other, format!("{}MHz AX.25 Frame does not contain an information field, {}", frequency, aprstext))));
            }

            // convert the information field for the APRS packet to UTF8 text
            let info = match String::from_utf8(infodata) {
                Ok(s) => s,
                Err(e) => {
                    let lossy_info_field = String::from_utf8_lossy(e.as_bytes());
                    return Err(Box::new(io::Error::new(ErrorKind::Other, format!("{}MHz Failed to convert to UTF-8: {}:{}. {}", frequency, aprstext, lossy_info_field, e))));
                },
            };

            // append the information field
            aprstext.push(':');
            aprstext.push_str(info.trim());

            // the APRS packet type
            let ptype: char = match info.chars().next() {
                Some(c) => c,
                None => return Err(Box::new(io::Error::new(ErrorKind::Other, "No APRS data type found.".to_string()))),
            };

            // return a new Packet structure
            Ok(RTPPacket {
                receivetime,
                is_satellite: SAT_FREQS.contains(&frequency),
                frequency,
                path: viapath,
                heardfrom,
                heard_direct,
                rfonly,
                ptype,
                source,
                destination,
                raw: aprstext,
                info,
            })
        },
        Err(e) => Err(Box::new(io::Error::new(ErrorKind::Other, format!("No RTP packet payload to parse: {:?}", e)))),
    }
}

