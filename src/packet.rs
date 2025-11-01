//#![allow(unused)]
use tokio::{net::{UdpSocket}, sync::broadcast, time::{interval, Duration}};
use tokio_util::sync::{CancellationToken};
use std::{io::{self, ErrorKind}, net::{Ipv4Addr, SocketAddr, ToSocketAddrs}, sync::Arc, error::Error, fmt};
use rtp_rs::{RtpReader};
use serde::{Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use chrono::{DateTime, Local, Utc};
use ax25::frame::{Ax25Frame, FrameContent};
//use aprs_parser::{AprsCst, AprsData, AprsPacket, AprsPosition, Callsign, Latitude, Longitude, Precision, Timestamp, Via, QConstruct};

// for logging
use log::{info, warn, error, debug};

// read in the configuration type
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

    // next() returns an Option. We can convert it to a Result using ok_or_else or similar.
    //addrs.next().ok_or_else(|| { Err(Box::new(io::Error::new(ErrorKind::Other, format!("No valid socket address found for, {}.", address))))
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


// Listens for RTP packets on a given multicast address and calls the parse_rtp_packet function
// with the parsed RTP header and payload.  Normally, this will never return - it loops forever.
pub async fn rtp_listener(data_channel: broadcast::Sender<DataItem>, token: CancellationToken, config: Arc<Config>) -> Result<(), Box<dyn Error>> {

    info!("Started");

    // subscribe the data channel
    let _data_stream = data_channel.subscribe();

    // get the address and port of the multicast end point we need to connect too
    let address = format!("{}:{}", config.rtp.host, config.rtp.port);

    // statistics
    let mut heard_direct = 0;
    let mut decode_errors = 0;
    let mut total_packets = 0;

    // data series 
    let mut packets_series = DataSeries {
        name: String::from("total_packets"),
        data: Vec::new(),
    };
    let mut heard_direct_series = DataSeries {
        name: String::from("heard_direct"),
        data: Vec::new(),
    };

    let mut decode_errors_series = DataSeries {
        name: String::from("decode_errors"),
        data: Vec::new(),
    };

    // the interval for when to send statistics 
    let mut time_interval = interval(Duration::from_secs(15));

    // convert address to a multicast address, then attempt to create a UDP socket bound to that address.
    let multicast_addr = get_multicast_socket_addr(address.as_str())?;
    let std_socket = bind_multicast(multicast_addr)?;

    // convert std library socket to a Tokio socket.
    let udp_socket = UdpSocket::from_std(std_socket)?;

    info!("Connected to RTP multicast address: {}", multicast_addr);

    // buffer where we store incoming bytes
    let mut buf = [0u8; 1500];

    // connected, now read continually from the socket
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
                packets_series.data.push(DataPoint { timestamp: the_time, value: total_packets, });
                heard_direct_series.data.push(DataPoint { timestamp: the_time, value: heard_direct, });
                decode_errors_series.data.push(DataPoint { timestamp: the_time, value: decode_errors, });

                // check that the length of the series is not longer than 100
                if packets_series.data.len() > 100 {
                    packets_series.data.remove(0);
                }
                if heard_direct_series.data.len() > 100 {
                    heard_direct_series.data.remove(0);
                }
                if decode_errors_series.data.len() > 100 {
                    decode_errors_series.data.remove(0);
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
                    error!("Failed to send statistics data to channel: {}", e);
                    break;
                }

                // reset counters
                heard_direct = 0;
                decode_errors = 0;
                total_packets = 0;

            },


            // read from the socket
            result = udp_socket.recv_from(&mut buf) => {
                match result {

                    // read was successful
                    Ok((num_bytes, _src_addr)) => {
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
                                        match data_channel.send(DataItem::Pkt(Packet::RTP(p))) {
                                            Ok(_a) => _a,
                                            Err(e) => {
                                                error!("Channel send failed: {}", e);
                                                break;
                                            },
                                        };
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

                    // socket read error
                    Err(e) => {
                        error!("RTP socket read failed: {}", e);
                        break;
                    },
                }
            },
        }
    }

    // drop the channel
    drop(data_channel);

    info!("Task ended.");

    Ok(())
}


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

            // start constructing the APRS packet
            let source: String = format!("{}", ax25_frame.source);
            let mut aprstext: String = format!("{}>{}", ax25_frame.source, ax25_frame.destination);

            // construct the viapath for the APRS packet
            let path_elements: Vec<String> = ax25_frame.route.iter().map(|p| p.repeater.to_string()).collect();
            let viapath = path_elements.join(",");
            aprstext = format!("{},{}", aprstext, viapath);

            // list of digipeater addresses that we filter out when checking if a packet has been
            // digipeated or not.
            let excluded_addrs = vec!["WIDE", "TCPIP", "NOGATE", "RFONLY", "SGATE"];

            // did this station hear this packet directly or was it digipeated?
            let heard_direct: bool = ax25_frame.route.iter().filter(|p| p.has_repeated && excluded_addrs.iter().all(|x| !p.repeater.to_string().contains(x))).count() == 0;
            let heardfrom: String = match heard_direct {
                true => source.clone(),
                false => {
                    let elems: Vec<String> = ax25_frame.route.iter().filter(|p| p.has_repeated && excluded_addrs.iter().all(|x| !p.repeater.to_string().contains(x))).map(|p| p.repeater.to_string()).collect();
                    match elems.last() {
                        Some(repeater) => repeater.to_string(),
                        None => source.clone(),
                    }
                }
            };

            // Check if this packet is RF only and should not be igated, we set this flag just in
            // case downstream consumers of this Packet need to check.
            let rfonly_addrs = vec!["TCPIP", "RFONLY", "NOGATE"];
            let rfonly: bool = ax25_frame.route.iter().filter(|p| rfonly_addrs.iter().all(|x| p.repeater.to_string().contains(x))).count() > 0;

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
            let info = match String::from_utf8(infodata.clone()) {
                Ok(s) => s,
                Err(e) => {
                    let lossy_info_field = String::from_utf8_lossy(&infodata[0..]);
                    return Err(Box::new(io::Error::new(ErrorKind::Other, format!("{}MHz Failed to convert to UTF-8: {}:{}. {}", frequency, aprstext, lossy_info_field, e))));
                },
            };

            // finally add on the information field
            aprstext = format!("{}:{}", aprstext, &info.trim());

            // the APRS packet type
            let ptype: char = match info.chars().next() {
                Some(c) => c,
                None => return Err(Box::new(io::Error::new(ErrorKind::Other, format!("No APRS data type found.")))),
            };

            // return a new Packet structure
            Ok(RTPPacket {
                receivetime: receivetime,
                is_satellite: is_satellite(&frequency),
                frequency: frequency,
                path: viapath,
                heardfrom: heardfrom,
                heard_direct: heard_direct,
                rfonly: rfonly,
                ptype: ptype,
                source: source,
                destination: ax25_frame.destination.to_string(),
                raw: aprstext,
                info: info.to_string(),
            })
        },
        Err(e) => Err(Box::new(io::Error::new(ErrorKind::Other, format!("No RTP packet payload to parse: {:?}", e)))),
    }
}


// check if this packet is satellite based/destined
fn is_satellite(f: &f64) -> bool {

    // check if this a packet from or destined too a satellite
    let sat_freqs = vec![145.825];

    //let known_sats = vec!["RS0ISS", "DP0SNX", "A55BTN"];
    //if sat_freqs.contains(&f) || known_sats.contains(&p.source.as_str()) {
    //
    
    // just checking the frequency for now...
    if sat_freqs.contains(f) {
        true
    }
    else {
        false
    }
}

