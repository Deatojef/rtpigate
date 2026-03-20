use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, fs, ops::Add};
use chrono::{DateTime, Local};

use crate::error::RtpigateError;

use crate::ka9q::Packet;

#[derive(Debug, Clone)]
pub enum DataItem {
    Pkt(Packet),
    Tlm(AppTelemetry),
}

#[derive(Debug, Clone)]
pub enum AppTelemetry {
    PacketStatus(PacketTelemetry),
    AprsisStatus(AprsisTelemetry),
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct PacketTelemetry {
    pub name: String,
    pub timestamp: DateTime<Local>,
    pub microsecs: f64,
    pub decode_errors: DataSeries<u32>,
    pub heard_direct: DataSeries<u32>,
    pub total_packets: DataSeries<u32>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct AprsisTelemetry {
    pub name: String,
    pub timestamp: DateTime<Local>,
    pub microsecs: f64,
    pub reconnects: DataSeries<u32>,
    pub packets_igated: DataSeries<u32>,
    pub packets_dropped: DataSeries<u32>,
    pub rf_received: DataSeries<u32>,
    pub inet_received: DataSeries<u32>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct DataSeries<T: Add> {
    pub name: String,
    pub data: VecDeque<DataPoint<T>>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct DataPoint<T: Add> {
    pub timestamp: DateTime<Local>,
    pub value: T,
}




//--------- configuration file definitions/handling --------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Config {
    pub station: StationConfig,
    pub location: Location,
    pub aprsis: AprsisConfig,
    pub rtp: RtpConfig,
    pub http: Option<HttpConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct HttpConfig {
    pub listen: Option<String>,
    pub frontend: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct StationConfig {
    pub callsign: Option<String>,
    pub name: Option<String>,
    pub timezone: Option<String>,
    pub verbose: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Location {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt: Option<f64>,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AprsisConfig {
    pub passcode: Option<String>,
    pub host: Option<String>,
    pub port: Option<u32>,
    pub enabled: Option<bool>,
    pub beaconing: Option<bool>,
    pub igating: Option<bool>,
    pub symbol: Option<String>,
    pub overlay: Option<String>,
    pub customfilter: Option<String>,
    pub threshold: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RtpConfig {
    pub host: String,
    pub port: u32,
}

/// Sanitized APRS-IS config for the frontend (passcode omitted)
#[derive(Serialize, Debug, Clone)]
pub struct AprsisConfigPublic {
    pub host: Option<String>,
    pub port: Option<u32>,
    pub enabled: Option<bool>,
    pub beaconing: Option<bool>,
    pub igating: Option<bool>,
    pub symbol: Option<String>,
    pub overlay: Option<String>,
    pub threshold: Option<u64>,
}

/// Sanitized config for the frontend (no secrets)
#[derive(Serialize, Debug, Clone)]
pub struct PublicConfig {
    pub station: StationConfig,
    pub location: Location,
    pub aprsis: AprsisConfigPublic,
    pub rtp: RtpConfig,
    pub started_at: Option<DateTime<Local>>,
}

impl Config {
    pub fn to_public(&self) -> PublicConfig {
        PublicConfig {
            station: self.station.clone(),
            location: self.location.clone(),
            aprsis: AprsisConfigPublic {
                host: self.aprsis.host.clone(),
                port: self.aprsis.port,
                enabled: self.aprsis.enabled,
                beaconing: self.aprsis.beaconing,
                igating: self.aprsis.igating,
                symbol: self.aprsis.symbol.clone(),
                overlay: self.aprsis.overlay.clone(),
                threshold: self.aprsis.threshold,
            },
            rtp: self.rtp.clone(),
            started_at: None,
        }
    }
}

pub trait APRSISLogin {
    fn aprsis_login_string(&self) -> String;
}

pub trait APRSISPasscode {
    fn compute_passcode(&self) -> i32;
    fn passcode_isvalid(&self) -> bool;
}


impl Config {

    // attempt to read the TOML syntax from the provided filename string returning a Config structure
    // if successful.
    pub fn from_file(filename: &str) -> Result<Config, RtpigateError> {

        // read in the config file
        let toml_string = fs::read_to_string(filename)?;

        // return the result
        Ok(toml::from_str::<Config>(&toml_string)?)
    }

    /// Validate configuration, returning a list of error messages.
    /// An empty list means the config is valid.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // Station callsign is always required
        match &self.station.callsign {
            Some(c) if c.is_empty() => errors.push("[station] callsign is empty".into()),
            None => errors.push("[station] callsign is required".into()),
            _ => {},
        }

        // RTP host/port are required (already non-optional in the struct, but validate content)
        if self.rtp.host.is_empty() {
            errors.push("[rtp] host is required".into());
        }
        if self.rtp.port == 0 {
            errors.push("[rtp] port must be > 0".into());
        }

        // Location validation — lat/lon ranges
        if let Some(lat) = self.location.lat {
            if !(-90.0..=90.0).contains(&lat) {
                errors.push(format!("[location] lat {} is out of range (-90 to 90)", lat));
            }
        }
        if let Some(lon) = self.location.lon {
            if !(-180.0..=180.0).contains(&lon) {
                errors.push(format!("[location] lon {} is out of range (-180 to 180)", lon));
            }
        }

        // If APRS-IS is enabled, validate its required fields
        if self.aprsis.enabled == Some(true) {
            match &self.aprsis.host {
                Some(h) if h.is_empty() => errors.push("[aprsis] host is empty but aprsis is enabled".into()),
                None => errors.push("[aprsis] host is required when aprsis is enabled".into()),
                _ => {},
            }
            if self.aprsis.port.is_none() {
                errors.push("[aprsis] port is required when aprsis is enabled".into());
            }

            // If beaconing is enabled, location is required
            if self.aprsis.beaconing == Some(true) {
                if self.location.lat.is_none() || self.location.lon.is_none() {
                    errors.push("[location] lat and lon are required when beaconing is enabled".into());
                }
                if self.location.alt.is_none() {
                    errors.push("[location] alt is required when beaconing is enabled".into());
                }
            }
        }

        // HTTP listen address validation
        if let Some(ref http) = self.http {
            if let Some(ref listen) = http.listen {
                if !listen.contains(':') {
                    errors.push(format!("[http] listen '{}' should be in host:port format", listen));
                }
            }
        }

        errors
    }
}

impl APRSISLogin for Config {
    fn aprsis_login_string(&self) -> String {

        let callsign = match &self.station.callsign {
            Some(c) => c,
            None => &String::from("N0CAL"),
        };

        // Only send the real passcode if it's valid; otherwise send -1 for read-only
        let passcode = if self.passcode_isvalid() {
            match &self.aprsis.passcode { Some(m) => m.clone(), None => String::from("-1") }
        } else {
            String::from("-1")
        };

        format!(
            "user {} pass {} vers {}\r\n",
            callsign,
            passcode,
            "1.0",
        )
    }
}

impl APRSISPasscode for Config {
    fn compute_passcode(&self) -> i32 {
        match &self.station.callsign {
            Some(c) => passcode(c),
            None => -1,
        }
    }

    fn passcode_isvalid(&self) -> bool {
        match &self.aprsis.passcode {
            Some(p) => match p.parse::<i32>() {
                Ok(i) => self.compute_passcode() == i,
                Err(_) => false,
            },
            None => false,
        }
    }
}


// function to compute the APRS-IS passcode of a provided string (i.e. a callsign).
fn passcode(callsign: &str) -> i32 {
    let mut hash: i32 = 0x73e2;

    // loop over each character within the callsign until we hit a hyphen or the end of the string
    for (i, c) in callsign.to_uppercase().char_indices() {

        if c == '-' {
            break;
        }

        let shift = match i % 2 { 0 => 8, _ => 0, };
        hash ^= (c as i32) << shift;
    }

    hash & 0x7fff
}
