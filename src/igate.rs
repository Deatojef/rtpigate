use std::{io::{self, ErrorKind}, error::Error};
use chrono::{Local, Utc, Timelike};
use tokio::{fs, io::AsyncWriteExt};

use log::debug;

use crate::config::Location;
use crate::ka9q::RTPPacket;

/// TOCALL value for this software. 'APZ' denotes experimental. 'JD1' denotes the version.
pub static TOCALL: &str = "APZJD1";


// ---- Packet filtering ----

/// Determine if a packet should be dropped (i.e. not igated).
/// Returns true if the packet should be dropped, false if it should be gated.
pub fn droppacket(p: &RTPPacket) -> bool {
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


// ---- Position beacon construction ----

/// Construct a position packet for beaconing to APRS-IS.
pub fn positpacket(l: &Location, callsign: &str, name: &str, symbol: &Option<String>, overlay: &Option<String>) -> Result<String, Box<dyn Error>> {

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

            // APRS symbols and overlays are convoluted nonsense.  Try and decipher...
            let overlay_string = match overlay {
                Some(o) => format!("{}", o),
                None => match symbol {
                    Some(s) => match s.chars().next() {
                        Some(c) => format!("{}", c),
                        None => String::from("/"),
                    },
                    None => String::from("/"),
                },
            };

            let symbol_string = match symbol {
                Some(s) => match s.chars().nth(1) {
                    Some(k) => format!("{}", k),
                    None => String::from("0"),
                },
                None => String::from("0"),
            };

            // construct the packet text
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


// ---- Telemetry ----

/// Read the telemetry sequence file and return the sequence integer contained within.
pub async fn read_telemetry_file(filename: &str) -> Result<u32, Box<dyn Error>> {
    use tokio::io::{AsyncBufReadExt, BufReader};

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

    let reader = BufReader::new(file);

    let first_line = match reader.lines().next_line().await? {
        Some(line) => line,
        None => return Err(Box::new(io::Error::new(io::ErrorKind::InvalidData, format!("File, {}, was empty or the first line could not be read.", filename)))),
    };

    let number = first_line.trim().parse::<u32>()?;
    Ok(number)
}

/// Write the provided sequence number to the filename provided.
pub async fn write_telemetry_seq(filename: &str, seq: u32) -> Result<u32, Box<dyn Error>> {
    fs::write(filename, format!("{}\n", seq)).await?;
    Ok(seq)
}

/// Create a telemetry file using the filename provided.
async fn create_telemetry_file(filename: &str) -> Result<u32, Box<dyn Error>> {
    let mut file = fs::File::create(filename).await?;
    file.write_all(b"0\n").await?;
    Ok(0)
}


// ---- APRS Telemetry encoding (quadratic coefficients) ----

#[derive(Debug, Clone)]
pub struct APRSQuadratic {
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub x: u32,
}

impl APRSQuadratic {
    pub fn new(orig_value: f64) -> APRSQuadratic {

        // if the original value is small (between -255 and +255) we forego the use of the "a" coefficient
        if orig_value <= 255.0 || orig_value >= -255.0 {

            let x = if orig_value >= 0.0 {
                orig_value.floor()
            } else {
                orig_value.ceil()
            };

            let a = 0.0;
            let b = 1.0;
            let c = ((orig_value - x) * 1000000.0).round() / 1000000.0;

            APRSQuadratic { a, b, c, x: x as u32 }
        }
        // in the case when the original value is larger than 255 (or less than -255)
        else {
            let x = 128.0;

            let (a, b, c) = if orig_value > 0.0 {
                let a = (orig_value / (x * x)).floor();
                let a_remainder = orig_value - a * x * x;
                let b = (a_remainder / x).floor();
                let b_remainder = a_remainder - b * x;
                let c = b_remainder;

                debug!("orig_value: {}, x: {}, a: {}, b: {}, c: {}, a_remainder: {}, b_remainder: {}", orig_value, x, a, b, c, a_remainder, b_remainder);
                (a, b, c)
            } else {
                let a = (orig_value / (x * x)).ceil();
                let a_remainder = orig_value - a * x * x;
                let b = (a_remainder / x).ceil();
                let b_remainder = a_remainder - b * x;
                let c = b_remainder;

                debug!("orig_value: {}, x: {}, a: {}, b: {}, c: {}, a_remainder: {}, b_remainder: {}", orig_value, x, a, b, c, a_remainder, b_remainder);
                (a, b, c)
            };

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

    /// Creates a series of APRS telemetry information strings that can then be wrapped in
    /// an APRS packet. These don't use any of the digital bits fields.
    pub fn to_aprs(&self, callsign: &String) -> Result<Vec<String>, Box<dyn Error>> {

        if self.telemetry.len() == 0 {
            return Err(Box::new(io::Error::new(ErrorKind::Other, format!("No telemetry analog items defined."))));
        }

        let mut telem_string = format!("T#{}", self.sequence);
        let mut eqn_string =   format!(":{: <9}:EQNS", callsign);
        let mut parm_string =  format!(":{: <9}:PARM", callsign);
        let mut unit_string =  format!(":{: <9}:UNIT", callsign);
        let bits_string =  format!(":{: <9}:BITS.00000000,{}", callsign, self.name);

        let mut i: u32 = 1;
        for analog_item in &self.telemetry {

            // aprs spec allows for up to 5 analog items
            if i > 5 {
                break;
            }

            telem_string = format!("{},{:03}", telem_string, analog_item.equation.x);

            eqn_string = format!("{}{}{},{},{}",
                eqn_string,
                match i { 1 => ".", _ => "," },
                analog_item.equation.a,
                analog_item.equation.b,
                analog_item.equation.c
            );

            parm_string = format!("{}{}{}",
                parm_string,
                match i { 1 => ".", _ => "," },
                analog_item.label
            );

            unit_string = format!("{}{}{}",
                unit_string,
                match i { 1 => ".", _ => "," },
                analog_item.units
            );

            i += 1;
        }

        // pad with zeros if we have less than 5 telemetry items
        for _n in i..5 {
            telem_string = format!("{},000", telem_string);
            eqn_string = format!("{},0,0,0", eqn_string);
        }

        // add a zero'd digital value and the report comment
        telem_string = format!("{},00000000,{}", telem_string, self.name);

        Ok(vec![telem_string, eqn_string, parm_string, unit_string, bits_string])
    }
}
