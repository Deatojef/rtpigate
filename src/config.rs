use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, fs, net::Ipv4Addr, ops::Add, time::{Duration, Instant}};
use chrono::{DateTime, Local, Utc};

use crate::error::RtpigateError;

use crate::stream::Packet;

#[derive(Debug, Clone)]
pub enum DataItem {
    Pkt(Packet),
    Tlm(AppTelemetry),
}

#[derive(Debug, Clone)]
pub enum AppTelemetry {
    PacketStatus(PacketTelemetry),
    AprsisStatus(AprsisTelemetry),
    SlicerStatus(SlicerTelemetry),
    StationStatus(StationTelemetry),
    GpsStatus(GpsTelemetry),
}

/// Maximum age of a GPS fix before it is considered stale and unusable for
/// beaconing. Keeps a mobile station from ever transmitting an outdated position.
pub const GPS_FRESHNESS: Duration = Duration::from_secs(30);

/// Latest position/fix from GPSD, shared (behind an RwLock) between `gpsd_task`
/// (writer) and `aprsis_task` (reader). `received_at` is a monotonic local clock
/// used for the freshness guard; GPSD's own `time` is display-only.
#[derive(Debug, Clone)]
pub struct GpsFix {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt_ft: Option<f64>,
    pub mode: u8,               // 1 = no fix, 2 = 2D, 3 = 3D
    pub sats_used: u32,
    pub sats_visible: u32,
    pub hdop: Option<f64>,
    pub time: Option<DateTime<Utc>>,
    pub received_at: Instant,
}

impl GpsFix {
    /// A fix is usable for beaconing only when it is recent, has at least a 2D
    /// solution, and carries a lat/lon.
    pub fn is_fresh(&self) -> bool {
        self.received_at.elapsed() < GPS_FRESHNESS
            && self.mode >= 2
            && self.lat.is_some()
            && self.lon.is_some()
    }
}

/// Snapshot of GPS health pushed to the frontend via the `gps_status` SSE event.
#[derive(Serialize, Debug, Clone)]
pub struct GpsTelemetry {
    pub name: String,                   // "gps_status"
    pub timestamp: DateTime<Local>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt_ft: Option<f64>,
    pub mode: u8,
    pub sats_used: u32,
    pub sats_visible: u32,
    pub hdop: Option<f64>,
    pub time: Option<DateTime<Utc>>,
    pub fresh: bool,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct PacketTelemetry {
    pub name: String,
    pub timestamp: DateTime<Local>,
    pub microsecs: f64,
    pub total_packets: DataSeries<u32>,
    pub heard_direct: DataSeries<u32>,
    pub digipeated: DataSeries<u32>,
    pub decode_errors: DataSeries<u32>,
    pub lifetime_total_packets: u64,
    pub lifetime_heard_direct: u64,
    pub lifetime_digipeated: u64,
    pub lifetime_decode_errors: u64,
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
    pub lifetime_rf_received: u64,
    pub lifetime_packets_igated: u64,
    pub lifetime_packets_dropped: u64,
    pub lifetime_reconnects: u64,

    // Per-reason drop breakdown — all counted toward `lifetime_packets_dropped`
    // as well, but exposed individually so silent gating regressions are visible.
    pub lifetime_drops_stale: u64,
    pub lifetime_drops_rfonly: u64,
    pub lifetime_drops_query: u64,
    pub lifetime_drops_thirdparty: u64,
    pub lifetime_drops_sat: u64,
    pub lifetime_drops_duplicate: u64,
    pub lifetime_drops_malformed: u64,

    // Packets that were dropped by the broadcast channel (RecvError::Lagged)
    // before reaching the gating logic. Distinct from `packets_dropped` because
    // these never had a chance to be evaluated.
    pub lifetime_lagged_drops: u64,
}

// One 15-second snapshot of per-slicer packet counts for the slicer waterfall.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SlicerInterval {
    pub timestamp: DateTime<Local>,
    pub counts: Vec<u32>,           // length == slicer_count
}

// Slicer-diversity telemetry: a rolling window of per-slicer demodulation
// counts. Each `SlicerInterval` is one heatmap row; `counts[i]` is how many
// packets demodulator slicer `i` recovered during that 15s window.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SlicerTelemetry {
    pub name: String,               // "slicer_statistics"
    pub timestamp: DateTime<Local>,
    pub microsecs: f64,
    pub slicer_count: usize,        // number of heatmap columns (decoder config)
    pub slicer_gains: Vec<f32>,     // per-slicer space-gain ladder (length slicer_count)
    pub intervals: VecDeque<SlicerInterval>,   // last 10, oldest-first
    pub lifetime_slicer_hits: Vec<u64>,        // per-slicer lifetime totals
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

#[derive(Serialize, Debug, Clone)]
pub struct StationEntry {
    pub callsign: String,
    pub transmitted_by: Option<String>,
    pub last_heard: DateTime<Local>,
    pub frequency: f64,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,
    pub heard_direct: bool,
    pub position_path: Vec<String>,
    pub position_hops: u32,
    pub altitude_path: Vec<String>,
    pub altitude_hops: u32,
    pub symbol_table: Option<char>,
    pub symbol_code: Option<char>,
    pub count: u64,
    pub count_direct: u64,
    pub count_digipeated: u64,
}

#[derive(Serialize, Debug, Clone)]
pub struct StationTelemetry {
    pub name: String,
    pub stations: Vec<StationEntry>,
    pub frequencies: Vec<FrequencyCount>,
}

#[derive(Serialize, Debug, Clone)]
pub struct FrequencyCount {
    pub frequency: String,
    pub count: u64,
}

//--------- configuration file definitions/handling --------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Config {
    pub station: StationConfig,
    pub location: Location,
    pub aprsis: AprsisConfig,
    pub stream: StreamConfig,
    pub satellite: Option<SatelliteConfig>,
    pub http: Option<HttpConfig>,
    pub gpsd: Option<GpsdConfig>,
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

/// Where the beacon position comes from.
///
/// - `Config` (default): use the static `[location]` lat/lon/alt below — the
///   fixed RF-antenna location. A fixed igate that happens to have a GPS attached
///   selects this, and GPSD is never consulted for beaconing.
/// - `Gpsd`: track a live fix from GPSD (mobile/portable). `[location]` is then
///   only a bootstrap/fallback; with no fresh fix the position beacon is skipped.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PositionSource {
    #[default]
    Config,
    Gpsd,
}

/// The station's configured location. `lat`/`lon`/`alt` describe the **RF-antenna
/// location** and are used to beacon when `source = "config"` (and as a GPSD
/// bootstrap/fallback when `source = "gpsd"`). `alt` is in feet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Location {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt: Option<f64>,
    #[serde(default)]
    pub source: PositionSource,
}

/// Connection and beaconing settings for the GPSD position source. Only consulted
/// when `[location] source = "gpsd"`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct GpsdConfig {
    pub host: Option<String>,            // default "localhost"
    pub port: Option<u16>,               // default 2947
    pub move_threshold_deg: Option<f64>, // default 0.0001 — beacon when |Δlat| or |Δlon| exceeds this
    pub min_beacon_secs: Option<u64>,    // default 30 — minimum spacing between movement beacons
}

impl GpsdConfig {
    pub const DEFAULT_HOST: &'static str = "localhost";
    pub const DEFAULT_PORT: u16 = 2947;
    pub const DEFAULT_MOVE_THRESHOLD_DEG: f64 = 0.0001;
    pub const DEFAULT_MIN_BEACON_SECS: u64 = 30;

    pub fn host(&self) -> String {
        match &self.host {
            Some(h) if !h.is_empty() => h.clone(),
            _ => Self::DEFAULT_HOST.to_string(),
        }
    }
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(Self::DEFAULT_PORT)
    }
    pub fn move_threshold_deg(&self) -> f64 {
        self.move_threshold_deg.unwrap_or(Self::DEFAULT_MOVE_THRESHOLD_DEG)
    }
    pub fn min_beacon_secs(&self) -> u64 {
        self.min_beacon_secs.unwrap_or(Self::DEFAULT_MIN_BEACON_SECS)
    }
}


/// How much positional precision to encode in beacon position packets via the
/// APRS 1.2 `!DAO!` extension.
///
/// - `Human` (default): human-readable `!Wxy!` form — one extra digit of minutes
///   (~1.85 m). Broadly legible and widely supported.
/// - `Base91`: base-91 `!wxy!` form — ~1/91 of the last base digit (~0.2 m). Best
///   for a well-surveyed fixed station or a high-precision receiver.
/// - `Off`: no `!DAO!` token; the base `ddmm.hh` position only (~18.5 m).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DaoMode {
    Off,
    #[default]
    Human,
    Base91,
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
    pub threshold: Option<u64>,
    pub dao: Option<DaoMode>,
}

/// The decoded-APRS multicast stream published by `aprs-streamd`. `group` is the
/// multicast group address and `port` the UDP port to subscribe on — these must
/// match the producer's emit destination. `interface` selects the local NIC to
/// join the group on for a multi-homed host (OS default when omitted);
/// `recv_buffer_bytes` optionally enlarges `SO_RCVBUF` to ride out bursts, the
/// consumer's only defense since the producer applies no backpressure.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct StreamConfig {
    pub group: Ipv4Addr,
    pub port: u16,
    #[serde(default)]
    pub interface: Option<Ipv4Addr>,
    #[serde(default)]
    pub recv_buffer_bytes: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SatelliteConfig {
    pub frequencies: Option<Vec<f64>>,
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
    pub dao: DaoMode,
}

/// Sanitized config for the frontend (no secrets)
#[derive(Serialize, Debug, Clone)]
pub struct PublicConfig {
    pub station: StationConfig,
    pub location: Location,
    pub aprsis: AprsisConfigPublic,
    pub stream: StreamConfig,
    pub satellite_frequencies: Vec<f64>,
    /// Effective GPSD settings (defaults resolved), present only when
    /// `[location] source = "gpsd"`.
    pub gpsd: Option<GpsdConfig>,
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
                dao: self.dao_mode(),
            },
            stream: self.stream.clone(),
            satellite_frequencies: self.satellite_frequencies(),
            // expose the effective (defaults-resolved) GPSD settings only when it
            // is the position source
            gpsd: if self.location.source == PositionSource::Gpsd {
                let g = self.gpsd_config();
                Some(GpsdConfig {
                    host: Some(g.host()),
                    port: Some(g.port()),
                    move_threshold_deg: Some(g.move_threshold_deg()),
                    min_beacon_secs: Some(g.min_beacon_secs()),
                })
            } else {
                None
            },
            started_at: None,
        }
    }

    /// Returns the configured `!DAO!` precision mode for beacons, defaulting to
    /// human-readable when `[aprsis] dao` is omitted.
    pub fn dao_mode(&self) -> DaoMode {
        self.aprsis.dao.unwrap_or_default()
    }

    /// Returns the effective GPSD settings, falling back to defaults when the
    /// optional `[gpsd]` section is omitted.
    pub fn gpsd_config(&self) -> GpsdConfig {
        self.gpsd.clone().unwrap_or_default()
    }

    /// Returns the configured satellite frequencies, or a default of [145.825]
    /// if the [satellite] section is missing.
    pub fn satellite_frequencies(&self) -> Vec<f64> {
        self.satellite
            .as_ref()
            .and_then(|s| s.frequencies.clone())
            .unwrap_or_else(|| vec![145.825])
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

        // Stream group/port are required. The group must be a multicast address
        // (224.0.0.0/4) since we join it via IGMP; a unicast address here is a
        // configuration mistake that would silently receive nothing.
        if !self.stream.group.is_multicast() {
            errors.push(format!(
                "[stream] group {} is not a multicast address (224.0.0.0 – 239.255.255.255)",
                self.stream.group
            ));
        }
        if self.stream.port == 0 {
            errors.push("[stream] port must be > 0".into());
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

            // If beaconing is enabled with the static config source, a fixed
            // location is required. With the GPSD source the position comes from
            // the live fix, so the static lat/lon/alt are optional.
            if self.aprsis.beaconing == Some(true) && self.location.source == PositionSource::Config {
                if self.location.lat.is_none() || self.location.lon.is_none() {
                    errors.push("[location] lat and lon are required when beaconing is enabled".into());
                }
                if self.location.alt.is_none() {
                    errors.push("[location] alt is required when beaconing is enabled".into());
                }
            }
        }

        // GPSD validation — only meaningful when GPSD is the position source
        if self.location.source == PositionSource::Gpsd {
            if let Some(ref gpsd) = self.gpsd {
                if let Some(threshold) = gpsd.move_threshold_deg {
                    if threshold <= 0.0 {
                        errors.push(format!("[gpsd] move_threshold_deg {} must be > 0", threshold));
                    }
                }
                if let Some(secs) = gpsd.min_beacon_secs {
                    if secs == 0 {
                        errors.push("[gpsd] min_beacon_secs must be > 0".into());
                    }
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


#[cfg(test)]
mod tests {
    use super::*;

    fn fix(mode: u8, lat: Option<f64>, lon: Option<f64>, age: Duration) -> GpsFix {
        GpsFix {
            lat, lon, alt_ft: None, mode,
            sats_used: 0, sats_visible: 0, hdop: None, time: None,
            received_at: Instant::now() - age,
        }
    }

    #[test]
    fn fresh_3d_fix_is_usable() {
        assert!(fix(3, Some(40.0), Some(-103.0), Duration::from_secs(1)).is_fresh());
    }

    #[test]
    fn stale_fix_is_not_fresh() {
        // older than GPS_FRESHNESS even though it is a valid 3D fix
        assert!(!fix(3, Some(40.0), Some(-103.0), GPS_FRESHNESS + Duration::from_secs(5)).is_fresh());
    }

    #[test]
    fn nofix_and_missing_coords_are_not_fresh() {
        assert!(!fix(1, Some(40.0), Some(-103.0), Duration::from_secs(1)).is_fresh()); // no fix
        assert!(!fix(3, None, Some(-103.0), Duration::from_secs(1)).is_fresh());       // missing lat
    }

    #[test]
    fn position_source_defaults_to_config() {
        assert_eq!(PositionSource::default(), PositionSource::Config);
    }

    #[test]
    fn gpsd_config_applies_defaults_when_empty() {
        let g = GpsdConfig::default();
        assert_eq!(g.host(), "localhost");
        assert_eq!(g.port(), 2947);
        assert_eq!(g.min_beacon_secs(), 30);
        assert!((g.move_threshold_deg() - 0.0001).abs() < f64::EPSILON);
    }
}
