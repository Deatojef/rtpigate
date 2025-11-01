//#[allow(unused)]
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, fs, net::{tcp, TcpStream}, sync::broadcast, time::{interval, Duration}};
use tokio_util::sync::{CancellationToken};
use std::{str, io::{self, ErrorKind}, net::{ToSocketAddrs}, sync::Arc, error::Error};
use chrono::{Local, Utc, Timelike };

// for logging
use log::{info, warn, error, debug};

// read in local types
use crate::config::{Config, APRSISLogin, APRSISPasscode, AppTelemetry, DataSeries, DataPoint, AprsisTelemetry, DataItem, Location};
use crate::packet::{RTPPacket, InetPacket, Packet};


// the tocall value for this software. 'APZ' denotes experimental.  'JD1' denotes the version. 
static TOCALL: &str = "APZJD1";

/*
fn print_hex_and_ascii(bytes: &Vec<u8>) -> String {
    let mut result = String::new();
    result.push_str("[ ");

    for (i, &b) in bytes.iter().enumerate() {
        if b >= 32 && b <= 126 {
            result.push_str(&format!("0x{:02x} ({})", b, b as char));
        } else {
            result.push_str(&format!("0x{:02x} (NA)", b));
        }

        if i < bytes.len() - 1 {
            result.push_str(", ");
        }
    }

    result.push_str(" ]");
    result
}
*/


// igating task
pub async fn aprsis_task(data_channel: broadcast::Sender<DataItem>, token: CancellationToken, config: Arc<Config>) -> Result<(), Box<dyn Error>> {

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
    let mut rf_received_series = DataSeries {
        name: String::from("rf_received"),
        data: Vec::new(),
    };
    
    // data series 
    let mut inet_received_series = DataSeries {
        name: String::from("inet_received"),
        data: Vec::new(),
    };

    // data series 
    let mut dropped_series = DataSeries {
        name: String::from("packets_dropped"),
        data: Vec::new(),
    };

    // data series 
    let mut igated_series = DataSeries {
        name: String::from("packets_igated"),
        data: Vec::new(),
    };

    // data series 
    let mut reconnect_series = DataSeries {
        name: String::from("aprsis_reconnects"),
        data: Vec::new(),
    };

    // the interval for when to send statistics 
    let mut telemetry_interval = interval(Duration::from_secs(15));


    // get the hostname of the APRS-IS host
    let host: String = match &config.aprsis.host {
        Some(h) => h.clone(),
        None => {
            return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to determine APRS-IS host"))));
        },
    };

    // get the port number of the connection to the APRS-IS host
    let port: u32 = match &config.aprsis.port {
        Some(p) => *p,
        None => {
            return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to determine APRS-IS port number:"))));
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
            return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to determine this station's callsign"))));
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

    // interval object that we'll use to trigger a set of telemetry packets to be sent to the
    // APRS-IS server
    let mut time_interval = interval(Duration::from_secs(telemetry_threshold));

    // telemetry sequence filename
    let telemetry_file = "/tmp/telem-seq.txt";

    // sequence number 
    let mut sequence: u32 = read_telemetry_file(telemetry_file).await?;


    // Basic flow of the aprsis_thread 
    // ------------------------------
    // outer loop
    //     - connect to aprs-is
    //     - read initial version string from server
    //     - send passcode
    //
    //     inner loop
    //         - send telemetry data to aprs-is if enough time has elapsed (5mins?)
    //         - compare current to _prev location and send posit packet to aprs-is if lat/lon has changed by > 0.0001 degs or enough time has elapsed (5mins?)
    //           when done, then save the current location to _prev.
    //         - read from channel loop (read from channel try_recv until nothing left to read)
    //             - read packet/location data from channel 
    //                 - if location data, then save to structure
    //                 - if packet data, then filter (ex. rfonly, satellite, etc.)
    //                     - send to aprs-is server if packet passes filter
    //         - if nothing was read from try_recv, then delay for a few milliseconds.
    //
    //     - close the connection to aprs-is server (presumably because an error occured as we've
    //       fallen out of the inner loop)
    //
    //     - if there was an error reading from the channel, then just return.
    //

    // outer aprs-is connection loop
    loop {

        // check if the main program has requested a shutdown.  If yes, then break out of this outer loop
        if token.is_cancelled() {
            break;
        }

        // convert the address to socket address
        let mut addrs = match address.to_socket_addrs() {
            Ok(a) => a,
            Err(e) => return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to parse address, {}. {}", address, e)))),
        };

        // next() returns an Option. We can convert it to a Result using ok_or_else or similar.
        let sock_addr = addrs.next().ok_or_else(|| { Box::new(io::Error::new(ErrorKind::Other, format!("No valid socket address found for, {}.", address)))})?;

        // create a TCP stream 
        let socket: TcpStream = TcpStream::connect(sock_addr).await?;

        // split the socket into read and write halves
        let (read_half, mut write_half) = socket.into_split();

        // buf reader to read from the socket
        let mut read_stream = BufReader::new(read_half);

        // try and log into the APRS-IS server
        tokio::select! {

            // was this thread canceled?
            _ = token.cancelled() => {
                break;
            },

            // login to the aprs-is server
            result = login_to_aprsis(&mut write_half, &mut read_stream, &loginstring, &host, &port) => {
                match result {
                    Ok(login) => {
                        if login {
                            info!("Login to {}:{} successful", host, port);
                        }
                        else {
                            error!("Login to {}:{} failed", host, port);
                            break;
                        }
                    },
                    Err(e) => {
                        error!("Error trying to log into {}:{}: {}", host, port, e);
                        break;
                    }
                }

            } // result = login_to_aprsis(
              
        } // tokio::select

        // inner loop:  loop forever reading data from the channels and the aprs-is server
        loop {

            // read buffer for data coming from the APRS-IS server
            //let mut raw = String::new();
            let mut raw = Vec::new();

            tokio::select! {

                // was this thread canceled?
                _ = token.cancelled() => {
                    break;
                },

                // send statistics to the data channel
                _ = telemetry_interval.tick() => {

                    // get the current time
                    let the_time = Local::now();

                    // add data points to series
                    rf_received_series.data.push(DataPoint { timestamp: the_time, value: stats_rf_received, });
                    inet_received_series.data.push(DataPoint { timestamp: the_time, value: stats_inet_received, });
                    dropped_series.data.push(DataPoint { timestamp: the_time, value: packets_dropped, });
                    igated_series.data.push(DataPoint { timestamp: the_time, value: packets_igated, });
                    reconnect_series.data.push(DataPoint { timestamp: the_time, value: 0, });

                    // check that the length of the series is not longer than 100
                    if rf_received_series.data.len() > 100 {
                        rf_received_series.data.remove(0);
                    }
                    if inet_received_series.data.len() > 100 {
                        inet_received_series.data.remove(0);
                    }
                    if dropped_series.data.len() > 100 {
                        dropped_series.data.remove(0);
                    }
                    if igated_series.data.len() > 100 {
                        igated_series.data.remove(0);
                    }
                    if reconnect_series.data.len() > 100 {
                        reconnect_series.data.remove(0);
                    }

                    // get the current time
                    let now = Utc::now();

                    // and in microsecs since the epoch
                    let microsecs: f64 = now.timestamp_micros() as f64 / 1000000.0;

                    // create a new struct to hold data statistics
                    let data = AprsisTelemetry {
                        name: String::from("aprsis_statistics"),
                        timestamp: the_time,
                        microsecs: microsecs,
                        reconnects: reconnect_series.clone(),
                        rf_received: rf_received_series.clone(),
                        inet_received: inet_received_series.clone(),
                        packets_dropped: dropped_series.clone(),
                        packets_igated: igated_series.clone(),
                    };

                    // Send statistics to the channel
                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::AprsisStatus(data))) {
                        error!("Failed to send statistics data to channel: {}", e);
                        break;
                    }

                    // reset counters
                    packets_dropped = 0;
                    packets_igated = 0;
                    stats_rf_received = 0;
                    stats_inet_received = 0;

                },

                // check if there's anything we can read from the APRS-IS server
                //read_result = read_stream.read_line(&mut raw) => {
                read_result = read_stream.read_until(b'\n', &mut raw) => {
                    match read_result {
                        Ok(_numbytes) => {

                            // check if the first character is a "#" comment.
                            let s = String::from_utf8_lossy(&raw);
                            match s.chars().next() {
                                Some(x) => match x {
                                    '#' => debug!("{}:{}: {}", host, port, s.trim_end()),
                                    _ => {

                                            debug!("trying to process packet from aprsis: {}", &s);

                                            // example packet
                                            // KK0X-10>APMI04,TCPIP*,qAS,KK0X:@171140z3934.15N/10455.05W-WX3in1Mini U=12.4V.

                                            // Convert to a string slice and search for the first '>' and ':' characters
                                            if let Ok(p) = str::from_utf8(&raw) {

                                                // trim off any trailing \r \n or whitespace.
                                                let packet_text = p.trim();

                                                // Find the starting index of the ">" character
                                                if let Some(source_delim) = packet_text.find(">") {

                                                    // Find the starting index of the ">" character
                                                    if let Some(info_delim) = packet_text.find(":") {

                                                        // the callsign
                                                        let source = &packet_text[0..source_delim];

                                                        // the info portion
                                                        let info = &packet_text[info_delim..];

                                                        // if there was a source and an information portion to the packet
                                                        if source.len() > 0 && info.len() > 0 {

                                                            if let Some(ptype) = info.chars().next() {
                                                                let packet = InetPacket {
                                                                    receivetime: Local::now(),
                                                                    info: format!("{}", info),
                                                                    source: format!("{}", source),
                                                                    ptype: ptype as char,
                                                                    raw: format!("{}", packet_text),
                                                                    aprsaddress: format!("{}", &address),
                                                                };

                                                                // attempt to send this packet to the channel so downstream consumers can process this packet.
                                                                match data_channel.send(DataItem::Pkt(Packet::Inet(packet))) {
                                                                    Ok(_a) => {
                                                                        debug!("---.---MHz Direct: 0  {}", packet_text);
                                                                        stats_inet_received += 1;
                                                                    },
                                                                    Err(e) => {
                                                                        error!("Channel send failed: {}", e);
                                                                        break;
                                                                    },
                                                                };
                                                            }
                                                        }
                                                    }
                                                }
                                            } // if let Ok(packet_text) = str::from_utf8(&raw) 
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

                                        // should this packet be dropped?
                                        if droppacket(&p) {
                                            warn!("dropping packet: {}MHz rfonly: {} Direct: {}  {}", p.frequency, p.rfonly as u32, p.heard_direct as u32, p.raw);

                                            // update dropped statistics counter;
                                            dropped += 1;
                                            packets_dropped += 1;
                                        }

                                        // try and igate the packet
                                        else {

                                            // reform packet into a string suitable for xmitting to an APRS-IS server
                                            let packet_text = p.for_rxigate(&callsign);

                                            // log this
                                            info!("Igating:  {}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                                            // write data to the socket
                                            write_half.write_all(format!("{}\r\n", packet_text).as_bytes()).await?;

                                            // update the sent_to_igate counter
                                            packets_igated += 1;
                                        }
                                    }

                                },
                                _ => (),
                            }
                        },
                        _ => (),
                    }
                }, // message = data_stream.recv()

                // wake up periodically to transmit telemetry data to the APRS-IS server.
                _ = time_interval.tick() => {

                    // only sends data to APRS-IS if beaconing is configured and igating.
                    if beaconing && rw {

                        // create a posit packet for sending to the APRS-IS server
                        let posit_text = match positpacket(&location, &callsign, &station_name, &symbol, &overlay) {
                            Ok(p) => p,
                            Err(e) => return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Error in trying to create position packet. {}", e)))),
                        };

                        // transmit this position packet to the aprsis server.
                        info!("xmitting: {}", posit_text);
                        write_half.write_all(format!("{}\r\n", posit_text).as_bytes()).await?;


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

                        // create a new Telemetry struct to hold the telem
                        let telem = Telemetry {
                            telemetry: items,
                            name: String::from("Telemetry Report"),
                            sequence: sequence
                        };

                        // convert the telemetry to a vector of info_strings
                        let packets = telem.to_aprs(&callsign)?;
                        debug!("{:?}", packets);

                        // transmit this telemetry to the APRS-IS server
                        for info_string in &packets {

                            // construct the packet_text for sending to the APRS-IS server
                            let packet_text = format!("{}>{},TCPIP*:{}", callsign, TOCALL, info_string);

                            info!("xmitting: {}", packet_text);
                            write_half.write_all(format!("{}\r\n", packet_text).as_bytes()).await?;
                        }

                        // increment the sequence number
                        sequence += 1;

                        // save the sequence to a file
                        sequence = write_telemetry_seq(telemetry_file, sequence).await?;

                        // clear stats
                        rf_received = 0;
                        dropped = 0;
                        heard_direct = 0;
                        received_other = 0;
                        received_sat = 0;
                    }
                } // _ = interval.tick() 

            } // tokio::select

        } // inner loop
          
    } // outer loop
      
    info!("Task ended.");

    Ok(())
}


// using the open socket and bufreader stream, attempt to log into the APRS-IS server
async fn login_to_aprsis(writer: &mut tcp::OwnedWriteHalf, reader: &mut BufReader<tcp::OwnedReadHalf>, loginstring: &str, host: &str, port: &u32) -> Result<bool, Box<dyn Error>> {

    // read the version line from the aprsis server to ensure a proper connection 
    let mut raw = String::new();

    // read a line from the BufReader (connected to the APRS-IS server)
    match reader.read_line(&mut raw).await {
        Ok(_numbytes) => { 
            debug!("{}:{}: {}", host, port, raw.trim_end());

            // send the login string to the aprs-is server
            writer.write_all(loginstring.as_bytes()).await?;
            writer.flush().await?;

            // read the response from APRS-IS
            let mut r = String::new();

            // wait on the result back from the APRS-IS server
            match reader.read_line(&mut r).await {
                Ok(_n) => {
                    debug!("{}:{}: {}", host, port, r.trim_end());
                },

                // was there an error reading from the APRS-IS server?
                Err(e) => {
                    error!("Unable to read from {}:{}", host, port);
                    Err(format!("Error in to {}:{}. {}", host, port, e))?;
                }
            }
        },

        // was there an error reading from the APRS-IS server?
        Err(e) => {
            error!("Unable to read from {}:{}", host, port);
            Err(format!("Error reading from {}:{}. {}", host, port, e))?;
        }
    }

    return Ok(true);
}

// construct a posit packet for beaconing to aprsis
fn positpacket(l: &Location, callsign: &str, name: &str, symbol: &Option<String>, overlay: &Option<String>) -> Result<String, Box<dyn Error>> {

    match (l.alt, l.lat, l.lon) {
        (Some(alt_ft), Some(lat), Some(lon)) => {

            // check for valid lat/lon/alt positions
            if alt_ft <= 0.0 || lat == 0.0 || lon == 0.0 {
                return Err(Box::new(io::Error::new(ErrorKind::Other, format!("positpacket: Invalid lat/lon/alt."))));
            }

            // the time components
            let dt = Utc::now();
            let hours = dt.hour();
            let minutes = dt.minute();
            let seconds = dt.second();

            // remove any negative degrees
            let abs_lat = lat.abs();
            let abs_lon = lon.abs();

            // directions
            let lat_ns = if lat >= 0.0 { 'N' } else { 'S' };
            let lon_ew = if lon >= 0.0 { 'E' } else { 'W' };

            // convert lat & lon to degrees, decimal minutes
            let lat_d = abs_lat.trunc();
            let lon_d = abs_lon.trunc();
            let lat_m = (abs_lat - lat_d) * 60.0;
            let lon_m = (abs_lon - lon_d) * 60.0;

            // For APRS, the position report represents latitude as ddmm.ssN or ddmm.ssS
            // For APRS, the position report represents longitude as dddmm.ssWor dddmm.ssE
            let lat_string = format!("{:02}{:05.2}{}", lat_d, lat_m, lat_ns);
            let lon_string = format!("{:03}{:05.2}{}", lon_d, lon_m, lon_ew);

            // if this is a mobile station, then need to add course/speed
            //
            // skipping for now...
            //
            //

            // example information field for an Rx-Only igate (i.e. gateway symbol)
            // :/153842h3920.92NR10447.86W&/A=006700Multifreq igate/satgate


            // APRS symbols and overlays are convoluted nonsense.  Try and decipher...
            // ------------------------- symbol & overlay decipher ----------------
            let overlay_string = match overlay {
                Some(o) => format!("{}", o),
                None => match symbol {      // no overlay, so determine APRS symbol table (i.e. '/' or '\') from first char of symbol
                    Some(s) => match s.chars().next() {
                        Some(c) => format!("{}", c),
                        None => String::from("/"),    // no first char???  default to the primary symbol table
                    },
                    None => String::from("/"),        // no symbol??       default to the primary symbol table
                },
            };


            // The incoming symbol should be in the form of [symbol table][symbol char].
            // For example:  /k  - is the primary symbol table, truck icon
            // For example:  \0  - is the alternate symbol table, circle icon
            let symbol_string = match symbol {
                Some(s) => match s.chars().nth(1) {
                    Some(k) => format!("{}", k),
                    None => String::from("0"),  // default to a circle overlaid with '0'
                },
                None => String::from("0"),  // default to a circle overlaid with '0'
            };
                
            // --------------------------------------------------------------------

            // construct the packet text
            // uses an Rx-Only igate symbol for now...
            let packet_text = format!(
                "{}>{},TCPIP*:/{:02}{:02}{:02}h{}{}{}{}/A={:06.0}{}",
                callsign,
                TOCALL,
                hours, 
                minutes,
                seconds,
                lat_string,
                overlay_string,
                lon_string, 
                symbol_string,
                alt_ft,
                name
            );

            Ok(packet_text)
        },

        _ => {
            Err(Box::new(io::Error::new(ErrorKind::Other, format!("positpacket: Invalid lat/lon/alt."))))
        }
    }
}



// read the telemetry file and return the sequence integer contained within
async fn read_telemetry_file(filename: &str) -> Result<u32, Box<dyn Error>> {

    debug!("Reading telemetry file, {}", filename);

    // open the telemetry file
    let file = match fs::File::open(filename).await {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                create_telemetry_file(filename).await?;
                fs::File::open(filename).await?
            },
            _other => {
                return Err(Box::new(io::Error::new(ErrorKind::Other, format!("Unable to create telemetry sequence file, {}: {}", filename, e))));
            },
        },
    };

    // create a new buffered reader for reading the contents of the file
    let reader = BufReader::new(file);

    // read in the first line from the telemetry file
    let first_line = match reader.lines().next_line().await? {
        Some(line) => line,
        None => return Err(Box::new(io::Error::new(io::ErrorKind::InvalidData, format!("File, {}, was empty or the first line could not be read.", filename)))),
    };

    // convert string to an integer
    let number = first_line.trim().parse::<u32>()?;

    Ok(number)
}


// write the provided sequence number to the 'filename' provided.
async fn write_telemetry_seq(filename: &str, seq: u32) -> Result<u32, Box<dyn Error>> {
    fs::write(filename, format!("{}\n", seq)).await?;
    Ok(seq)
}


// create a telemetry file using the filename provided
async fn create_telemetry_file(filename: &str) -> Result<u32, Box<dyn Error>> {
    let mut file = fs::File::create(filename).await?;
    file.write_all(b"0\n").await?;
    Ok(0)
}

// determine if a packet should be igated or droppped
// true:   returns true if the packet should be dropped (i.e. not igated)
// false:  returns false if the packet shoudl not be dropped (i.e. igated)
fn droppacket(p: &RTPPacket) -> bool {
    // the age threshold in seconds.  Packets older than this are dropped.
    let age_threshold = 30;

    // get the current timestamp
    let current_time = Local::now().timestamp();

    // get the packet's receive time
    let packet_time = p.receivetime.timestamp();

    // compare the two times, if > 30s then the packet should be dropped as too much time has
    // elapsed since its reception
    if current_time - packet_time > age_threshold {
        return true;
    }

    // if this is an "RF Only" packet or it's third party traffic, the we don't igate it.
    if p.rfonly || p.ptype == '{' {
        return true;
    }

    // This check is more involved that the simple is_satellite field within the RTPPacket struct,
    // as we neeed to determine if this satellite packet was digipeated or not - we don't igate
    // satellite packets heard directly unless they were from the satellites themselves.
    let sat_freqs = vec![145.825];
    let known_sats = vec!["RS0ISS", "DP0SNX", "A55BTN"];
    if p.heard_direct && sat_freqs.contains(&p.frequency) && !known_sats.contains(&p.source.as_str()) {
        true
    }

    // for everything else we igate it.
    else {
        false
    }
}


#[derive(Debug, Clone)]
pub struct APRSQuadratic {
    pub a: f64,   // coeficient for the x^2 term
    pub b: f64,   // coeficient for the x term
    pub c: f64,   // y-intercept
    pub x: u32,   // the integer value of x, that when used with the coef's and the quadratic
                  // formula, results in the original f64 value.
}

impl APRSQuadratic {
    pub fn new(orig_value: f64) -> APRSQuadratic {

        let mut x: f64 = 0.0;
        let mut a: f64 = 0.0;
        let mut b: f64 = 0.0;
        let mut c: f64 = 0.0;

        // if the original value is small (between -255 and +255) the we forego the use of the "a"
        // coefficient
        if orig_value <= 255.0 || orig_value >= -255.0 {

            // truncate the original value
            if orig_value >= 0.0 {
                x = orig_value.floor();
            }
            else {
                x = orig_value.ceil();
            }

            // the coefficients
            a = 0.0; // set to 0 as orig_value is too small
            b = 1.0; // set to 1 
            c = ((orig_value - x) * 1000000.0).round() / 1000000.0;   // the fractional residual, rounded to 6 digits

            // return a new APRSQuadratic struct
            APRSQuadratic {
                a: a,
                b: b,
                c: c,
                x: x as u32,
            }
        }

        // in the case when the original value is larger tha 255 (or less than -255)
        else {

            // arbitrary value for x
            x = 128.0;

            // if the original value is > 0.  
            if orig_value > 0.0 {

                // the 'a' coefficient (rounded to 6 digits)
                a = (orig_value / (x*x)).floor();
                let a_remainder: f64 = orig_value - a * x*x;

                // the 'b' coefficient (rounded to 6 digits)
                b = (a_remainder / x).floor();
                let b_remainder: f64 = a_remainder - b * x;

                // the 'c' coefficient (rounded to 6 digits)
                c = b_remainder;

                debug!("orig_value: {}, x: {}, a: {}, b: {}, c: {}, a_remainder: {}, b_remainder: {}", orig_value, x, a, b, c, a_remainder, b_remainder);
            }

            // if the original value is < 0.
            else {

                // the 'a' coefficient (rounded to 6 digits)
                a = (orig_value / (x*x)).ceil();
                let a_remainder: f64 = orig_value - a * x*x;

                // the 'b' coefficient (rounded to 6 digits)
                b = (a_remainder / x).ceil();
                let b_remainder: f64 = a_remainder - b * x;

                // the 'c' coefficient (rounded to 6 digits)
                c = b_remainder;

                debug!("orig_value: {}, x: {}, a: {}, b: {}, c: {}, a_remainder: {}, b_remainder: {}", orig_value, x, a, b, c, a_remainder, b_remainder);
            }

            // return a new APRSQuadratic struct
            APRSQuadratic {
                a: (a * 1000000.0).round() / 1000000.0,
                b: (b * 1000000.0).round() / 1000000.0,
                c: (c * 1000000.0).round() / 1000000.0,
                x: x as u32,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnalogItem {
    pub equation: APRSQuadratic,
    pub label: String,
    pub units: String,
}


#[derive(Debug, Clone)]
pub struct Telemetry {
    pub telemetry: Vec<AnalogItem>, 
    pub name: String,
    pub sequence: u32,
}

impl Telemetry {

    // creates a series of information strings that can then be wrapped in an APRS packet (these
    // don't use any of the digital bits fields
    pub fn to_aprs(&self, callsign: &String) -> Result<Vec<String>, Box <dyn Error>> {

        // if there aren't any telemetry items in the list, then we need to return.
        if self.telemetry.len() == 0 {
            return Err(Box::new(io::Error::new(ErrorKind::Other, format!("No telemetry analog items defined."))));
        }

        // T#731,171,000,000,009,000,00000000,Telemetry
        // :N0JD-10  :EQNS.0,1,0,0,1,0,0,1,0,0,1,0.356725,0,0,0
        // :N0JD-10  :PARM.Rx10m,RxSat10m,PctDrop10m,PctRxDirect10m
        // :N0JD-10  :UNIT.Pkts,Pkts,%,%
        // :N0JD-10  :BITS.00000000,Telemetry


        // construct the beginnings of the telemetry strings
        let mut telem_string = format!("T#{}", self.sequence);
        let mut eqn_string =   format!(":{: <9}:EQNS", callsign);
        let mut parm_string =  format!(":{: <9}:PARM", callsign);
        let mut unit_string =  format!(":{: <9}:UNIT", callsign);
        let mut bits_string =  format!(":{: <9}:BITS.00000000,{}", callsign, self.name);

        // loop through each telemetry item, appending to the various strings
        let mut i: u32 = 1;
        for analog_item in &self.telemetry {

            // aprs spec allows for up to 5 analog items
            if i > 5 {
                break;
            }

            // the telemetry string
            telem_string = format!("{},{:03}",
                telem_string,
                analog_item.equation.x
            );

            // the EQN string
            eqn_string = format!("{}{}{},{},{}",
                eqn_string,
                match i {
                    1 => ".",
                    _ => ",",
                },
                analog_item.equation.a,
                analog_item.equation.b,
                analog_item.equation.c
            );

            // the PARM string
            parm_string = format!("{}{}{}",
                parm_string,
                match i {
                    1 => ".",
                    _ => ",",
                },
                analog_item.label
            );

            // the UNIT string
            unit_string = format!("{}{}{}",
                unit_string,
                match i {
                    1 => ".",
                    _ => ",",
                },
                analog_item.units
            );

            i += 1;
        }

        //if we have less than 5 telemetry items, then we need to pad the telem and eqn strings with 0 entries
        for _n in i..5 {
            telem_string = format!("{},000", telem_string);
            eqn_string = format!("{},0,0,0", eqn_string);
        }

        // add a zero'd digital value and the report comment to the end of the telemetry string
        telem_string = format!("{},00000000,{}", telem_string, self.name);

        // now bundle up all of the strings and return the vector
        Ok(vec![telem_string, eqn_string, parm_string, unit_string, bits_string])
    }
}

