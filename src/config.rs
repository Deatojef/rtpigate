use serde::{self, Deserialize, Serialize};
use std::{fs, ops::Add, error::Error};
use chrono::{DateTime, Local};

use crate::packet::{Packet};

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
    pub data: Vec<DataPoint<T>>,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct StationConfig {
    pub callsign: Option<String>,
    pub name: Option<String>,
    pub timezone: Option<String>,
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
    pub fn from_file(filename: &str) -> Result<Config, Box<dyn Error>> {

        // read in the config file
        let toml_string = fs::read_to_string(filename)?;

        // return the result
        Ok(toml::from_str::<Config>(&toml_string)?)
    }

}

impl APRSISLogin for Config {
    fn aprsis_login_string(&self) -> String {

        let callsign = match &self.station.callsign {
            Some(c) => c,
            None => &String::from("N0CAL"),
        };

        format!(
            "user {} pass {} vers {}\r\n",
            callsign,
            match &self.aprsis.passcode { Some(m) => m.clone(), None => String::from("-1") },
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


/*
fn deserialize_string_or_number<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr + Deserialize<'de>,
    D: Deserializer<'de>,
    <T as FromStr>::Err: std::fmt::Display, // Ensure the error type implements Display
{
    let value: Value = Value::deserialize(deserializer)?;
    match value {
        Value::Number(num) => num.as_f64()
            .and_then(|n| T::from_str(&n.to_string()).ok())
            .ok_or_else(|| serde::de::Error::custom("Invalid number")),
        Value::String(s) => T::from_str(&s)
            .map_err(|e| serde::de::Error::custom(format!("Failed to parse string: {}", e))),
        _ => Err(serde::de::Error::custom("Expected a string or number")),
    }
}


fn deserialize_string_or_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Value::deserialize(deserializer)?;
    match value {
        Value::Bool(b) => Ok(b),
        Value::String(s) => {
            match s.to_lowercase().as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(serde::de::Error::custom("Expected 'true' or 'false'")),
            }
        },
        _ => Err(serde::de::Error::custom("Expected a boolean or string")),
    }
}


fn deserialize_null_from_empty_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Value::deserialize(deserializer)?;
    match value {
        Value::String(s) if s.is_empty() => Ok(None), // Map empty string to None
        Value::String(s) => Ok(Some(s)), // Map non-empty string to Some(value)
        Value::Null => Ok(None), // Map null to None
        _ => Err(serde::de::Error::custom("Expected a string or null")),
    }
}
*/
