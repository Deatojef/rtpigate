use chrono::{Utc, Timelike};
use std::time::Duration;
use tokio::{fs, io::AsyncWriteExt};

use log::debug;

use crate::config::Location;
use crate::error::RtpigateError;
use crate::ka9q::RTPPacket;

/// TOCALL value for this software. 'APZ' denotes experimental. 'JD1' denotes the version.
pub static TOCALL: &str = "APZJD1";


// ---- Packet filtering ----

/// Reason a packet was filtered out of the igating pipeline. Surfaced in logs
/// and broken out into per-reason counters so silent gating regressions show up
/// in `aprsis_statistics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    Stale,
    RfOnly,
    GenericQuery,
    ThirdPartyInternet,
    SatelliteDirect,
}

impl DropReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DropReason::Stale => "stale",
            DropReason::RfOnly => "rfonly",
            DropReason::GenericQuery => "query",
            DropReason::ThirdPartyInternet => "thirdparty_inet",
            DropReason::SatelliteDirect => "sat_direct",
        }
    }
}

/// Determine if a packet should be dropped (i.e. not igated).
/// Returns `Some(reason)` if the packet should be dropped, `None` if it should be gated.
pub fn droppacket(p: &RTPPacket) -> Option<DropReason> {
    // Stale-packet guard. Uses the monotonic Instant captured at parse time so
    // NTP corrections (or wall-clock skew) cannot spuriously age out packets.
    const AGE_THRESHOLD: Duration = Duration::from_secs(30);
    if p.received_instant.elapsed() > AGE_THRESHOLD {
        return Some(DropReason::Stale);
    }

    // if this is an "RF Only" packet, don't igate it.
    if p.rfonly {
        return Some(DropReason::RfOnly);
    }

    // drop generic query packets
    if p.ptype == '?' {
        return Some(DropReason::GenericQuery);
    }

    // drop third-party packets that contain internet markers in their inner header.
    // Match case-insensitively — uppercase is convention but malformed third-party
    // headers in the wild can be mixed case.
    if p.ptype == '}' {
        let inner = p.info.get(1..).unwrap_or("").to_ascii_uppercase();
        if inner.contains("TCPIP") || inner.contains("TCPXX") {
            return Some(DropReason::ThirdPartyInternet);
        }
    }

    // Satellite-frequency policy: gate iff the packet was digipeated by *anything*
    // (including unnamed fill-ins like WIDE1-1*) OR the source is a known satellite.
    // This uses `was_digipeated` rather than `heard_direct` because the latter
    // intentionally ignores WIDE-class digipeaters, which would let a fill-in-relayed
    // packet incorrectly fall into the "direct" branch.
    const SAT_FREQS: &[f64] = &[145.825];
    const KNOWN_SATS: &[&str] = &["RS0ISS", "NA1SS", "DP0ISS", "OR4ISS", "IR0ISS", "DP0SNX"];
    if !p.was_digipeated
        && SAT_FREQS.contains(&p.frequency)
        && !KNOWN_SATS.iter().any(|s| s.eq_ignore_ascii_case(&p.source))
    {
        return Some(DropReason::SatelliteDirect);
    }

    // for everything else we igate it.
    None
}


// ---- Position beacon construction ----

/// Construct a position packet for beaconing to APRS-IS.
pub fn positpacket(l: &Location, callsign: &str, name: &str, symbol: &Option<String>, overlay: &Option<String>) -> Result<String, RtpigateError> {

    match (l.alt, l.lat, l.lon) {
        (Some(alt_ft), Some(lat), Some(lon)) => {

            // check for valid lat/lon/alt positions
            if alt_ft <= 0.0 || lat == 0.0 || lon == 0.0 {
                return Err(RtpigateError::Validation("positpacket: Invalid lat/lon/alt".into()));
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
            Err(RtpigateError::Validation("positpacket: Missing lat/lon/alt".into()))
        }
    }
}


// ---- Telemetry ----

/// Read the telemetry sequence file and return the sequence integer contained within.
pub async fn read_telemetry_file(filename: &str) -> Result<u32, RtpigateError> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    debug!("Reading telemetry file, {}", filename);

    // open the telemetry file
    let file = match fs::File::open(filename).await {
        Ok(f) => f,
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                create_telemetry_file(filename).await?;
                fs::File::open(filename).await?
            },
            _ => return Err(RtpigateError::Io(e)),
        },
    };

    let reader = BufReader::new(file);

    let first_line = match reader.lines().next_line().await? {
        Some(line) => line,
        None => return Err(RtpigateError::Parse(format!("Telemetry file {} is empty", filename))),
    };

    let number = first_line.trim().parse::<u32>()?;
    Ok(number)
}

/// Write the provided sequence number to the filename provided.
pub async fn write_telemetry_seq(filename: &str, seq: u32) -> Result<u32, RtpigateError> {
    fs::write(filename, format!("{}\n", seq)).await?;
    Ok(seq)
}

/// Create a telemetry file using the filename provided.
async fn create_telemetry_file(filename: &str) -> Result<u32, RtpigateError> {
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
        if orig_value <= 255.0 && orig_value >= -255.0 {

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
    pub fn to_aprs(&self, callsign: &String) -> Result<Vec<String>, RtpigateError> {

        if self.telemetry.is_empty() {
            return Err(RtpigateError::Validation("No telemetry analog items defined".into()));
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
