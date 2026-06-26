use chrono::Local;
use std::time::Instant;

// We need to reference the crate's public items
use rtpigate::config::*;
use rtpigate::igate::*;
use rtpigate::ka9q::RTPPacket;

// ---- Helper: create a fresh RTPPacket for testing ----

fn make_packet() -> RTPPacket {
    RTPPacket {
        receivetime: Local::now(),
        received_instant: Instant::now(),
        raw: String::from("N0CALL>APRS,WIDE1-1:!4903.50N/07201.75W-"),
        info: String::from("!4903.50N/07201.75W-"),
        info_bytes: String::from("!4903.50N/07201.75W-").into_bytes(),
        info_invalid_bytes: 0,
        path: String::from("WIDE1-1"),
        digipeater_path: vec![],
        hops: 0,
        ptype: '!',
        source: String::from("N0CALL"),
        destination: String::from("APRS"),
        heard_direct: true,
        was_digipeated: false,
        heardfrom: String::from("N0CALL"),
        rfonly: false,
        frequency: 144.390,
        is_satellite: false,
        igated: false,
        object_name: None,
        latitude: Some(49.0583),
        longitude: Some(-72.0292),
        altitude_ft: None,
        slicer_mask: 0,
        twist: None,
    }
}

fn make_config(callsign: &str, passcode: &str) -> Config {
    Config {
        station: StationConfig {
            callsign: Some(callsign.to_string()),
            name: Some("Test Station".to_string()),
            timezone: None,
            verbose: None,
        },
        location: Location {
            lat: Some(39.0),
            lon: Some(-104.0),
            alt: Some(5000.0),
            source: PositionSource::Config,
        },
        aprsis: AprsisConfig {
            passcode: Some(passcode.to_string()),
            host: Some("noam.aprs2.net".to_string()),
            port: Some(14580),
            enabled: Some(true),
            beaconing: Some(true),
            igating: Some(true),
            symbol: Some("\\&".to_string()),
            overlay: Some("R".to_string()),
            threshold: Some(600),
            dao: None,
        },
        rtp: RtpConfig {
            host: "ax25.local".to_string(),
            port: 5004,
        },
        satellite: None,
        http: None,
        decoder: None,
        gpsd: None,
    }
}


// ========================================
// Passcode tests
// ========================================

#[test]
fn test_passcode_known_callsigns() {
    // Well-known passcode values that can be verified with online calculators
    let config = make_config("N0CALL", "13023");
    assert_eq!(config.compute_passcode(), 13023);
}

#[test]
fn test_passcode_valid() {
    let config = make_config("N0CALL", "13023");
    assert!(config.passcode_isvalid());
}

#[test]
fn test_passcode_invalid() {
    let config = make_config("N0CALL", "99999");
    assert!(!config.passcode_isvalid());
}

#[test]
fn test_passcode_negative_one_is_invalid() {
    let config = make_config("N0CALL", "-1");
    assert!(!config.passcode_isvalid());
}

#[test]
fn test_passcode_no_callsign() {
    let mut config = make_config("N0CALL", "13023");
    config.station.callsign = None;
    assert_eq!(config.compute_passcode(), -1);
    assert!(!config.passcode_isvalid());
}

#[test]
fn test_passcode_no_passcode() {
    let mut config = make_config("N0CALL", "13023");
    config.aprsis.passcode = None;
    assert!(!config.passcode_isvalid());
}

#[test]
fn test_passcode_with_ssid() {
    // Passcode should be same with or without SSID
    let config_base = make_config("N0CALL", "13023");
    let config_ssid = make_config("N0CALL-9", "13023");
    assert_eq!(config_base.compute_passcode(), config_ssid.compute_passcode());
}

#[test]
fn test_passcode_case_insensitive() {
    let config_upper = make_config("N0CALL", "13023");
    let config_lower = make_config("n0call", "13023");
    assert_eq!(config_upper.compute_passcode(), config_lower.compute_passcode());
}


// ========================================
// Login string tests
// ========================================

#[test]
fn test_login_string_valid_passcode() {
    let config = make_config("N0CALL", "13023");
    let login = config.aprsis_login_string();
    assert!(login.contains("user N0CALL"));
    assert!(login.contains("pass 13023"));
    assert!(login.ends_with("\r\n"));
}

#[test]
fn test_login_string_invalid_passcode_sends_negative_one() {
    let config = make_config("N0CALL", "99999");
    let login = config.aprsis_login_string();
    assert!(login.contains("pass -1"));
}

#[test]
fn test_login_string_no_passcode_sends_negative_one() {
    let mut config = make_config("N0CALL", "13023");
    config.aprsis.passcode = None;
    let login = config.aprsis_login_string();
    assert!(login.contains("pass -1"));
}


// ========================================
// Config validation tests
// ========================================

#[test]
fn test_valid_config_has_no_errors() {
    let config = make_config("N0CALL", "13023");
    assert!(config.validate().is_empty());
}

#[test]
fn test_missing_callsign_fails_validation() {
    let mut config = make_config("N0CALL", "13023");
    config.station.callsign = None;
    let errors = config.validate();
    assert!(!errors.is_empty());
    assert!(errors.iter().any(|e| e.contains("callsign")));
}

#[test]
fn test_empty_callsign_fails_validation() {
    let config = make_config("", "13023");
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("callsign")));
}

#[test]
fn test_lat_out_of_range_fails() {
    let mut config = make_config("N0CALL", "13023");
    config.location.lat = Some(91.0);
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("lat")));
}

#[test]
fn test_lon_out_of_range_fails() {
    let mut config = make_config("N0CALL", "13023");
    config.location.lon = Some(-181.0);
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("lon")));
}

#[test]
fn test_aprsis_enabled_without_host_fails() {
    let mut config = make_config("N0CALL", "13023");
    config.aprsis.host = None;
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("host")));
}

#[test]
fn test_beaconing_without_location_fails() {
    let mut config = make_config("N0CALL", "13023");
    config.location.lat = None;
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("lat") && e.contains("beaconing")));
}

#[test]
fn test_aprsis_disabled_skips_host_check() {
    let mut config = make_config("N0CALL", "13023");
    config.aprsis.enabled = Some(false);
    config.aprsis.host = None;
    config.aprsis.port = None;
    config.aprsis.beaconing = None;
    config.aprsis.igating = None;
    assert!(config.validate().is_empty());
}

#[test]
fn test_empty_rtp_host_fails() {
    let mut config = make_config("N0CALL", "13023");
    config.rtp.host = String::new();
    let errors = config.validate();
    assert!(errors.iter().any(|e| e.contains("rtp")));
}


// ========================================
// droppacket tests
// ========================================

#[test]
fn test_normal_packet_not_dropped() {
    let p = make_packet();
    assert!(droppacket(&p).is_none());
}

#[test]
fn test_rfonly_packet_dropped() {
    let mut p = make_packet();
    p.rfonly = true;
    assert_eq!(droppacket(&p), Some(DropReason::RfOnly));
}

#[test]
fn test_query_packet_dropped() {
    let mut p = make_packet();
    p.ptype = '?';
    p.info = String::from("?APRS?");
    assert_eq!(droppacket(&p), Some(DropReason::GenericQuery));
}

#[test]
fn test_third_party_with_tcpip_dropped() {
    let mut p = make_packet();
    p.ptype = '}';
    p.info = String::from("}N0CALL>APRS,TCPIP*:!4903.50N/07201.75W-");
    assert_eq!(droppacket(&p), Some(DropReason::ThirdPartyInternet));
}

#[test]
fn test_third_party_with_tcpxx_dropped() {
    let mut p = make_packet();
    p.ptype = '}';
    p.info = String::from("}N0CALL>APRS,TCPXX*:!4903.50N/07201.75W-");
    assert_eq!(droppacket(&p), Some(DropReason::ThirdPartyInternet));
}

#[test]
fn test_third_party_with_lowercase_tcpip_dropped() {
    // M3: marker substring matches must be case-insensitive
    let mut p = make_packet();
    p.ptype = '}';
    p.info = String::from("}N0CALL>APRS,tcpip*:!4903.50N/07201.75W-");
    assert_eq!(droppacket(&p), Some(DropReason::ThirdPartyInternet));
}

#[test]
fn test_third_party_without_internet_markers_not_dropped() {
    let mut p = make_packet();
    p.ptype = '}';
    p.info = String::from("}N0CALL>APRS,WIDE1-1:!4903.50N/07201.75W-");
    assert!(droppacket(&p).is_none());
}

#[test]
fn test_satellite_direct_non_sat_callsign_dropped() {
    let mut p = make_packet();
    p.frequency = 145.825;
    p.is_satellite = true;        // rtp_listener flags sat-frequency packets
    p.heard_direct = true;
    p.source = String::from("N0CALL");
    assert_eq!(droppacket(&p), Some(DropReason::SatelliteDirect));
}

#[test]
fn test_satellite_direct_known_sat_not_dropped() {
    let mut p = make_packet();
    p.frequency = 145.825;
    p.is_satellite = true;        // rtp_listener flags sat-frequency packets
    p.heard_direct = true;
    p.source = String::from("RS0ISS");
    assert!(droppacket(&p).is_none());
}

#[test]
fn test_satellite_direct_known_sat_lowercase_not_dropped() {
    // M3: callsign matching must be case-insensitive
    let mut p = make_packet();
    p.frequency = 145.825;
    p.is_satellite = true;        // rtp_listener flags sat-frequency packets
    p.heard_direct = true;
    p.source = String::from("rs0iss");
    assert!(droppacket(&p).is_none());
}

#[test]
fn test_satellite_digipeated_not_dropped() {
    let mut p = make_packet();
    p.frequency = 145.825;
    p.is_satellite = true;        // rtp_listener flags sat-frequency packets
    p.heard_direct = false;
    p.was_digipeated = true;
    p.source = String::from("N0CALL");
    assert!(droppacket(&p).is_none());
}

#[test]
fn test_satellite_wide_fillin_digipeated_not_dropped() {
    // L2 edge case: a sat-frequency packet whose only repeated path entry is a
    // WIDE-class fill-in. `heard_direct` reports true (because WIDE is in
    // EXCLUDED_ADDRS), but the strict `was_digipeated` flag is true — so the
    // sat filter must NOT drop it. Tests that the filter uses the strict flag,
    // not the convention-laden `heard_direct`.
    let mut p = make_packet();
    p.frequency = 145.825;
    p.is_satellite = true;        // rtp_listener flags sat-frequency packets
    p.heard_direct = true;        // artifact of EXCLUDED_ADDRS scrub
    p.was_digipeated = true;      // strict: a `*` is genuinely present in path
    p.source = String::from("N0CALL");  // not a known satellite
    assert!(droppacket(&p).is_none(), "WIDE-fill-in-digipeated sat packet must be gated");
}

#[test]
fn test_stale_packet_dropped() {
    let mut p = make_packet();
    // Push the monotonic instant 60 s into the past — the staleness check uses
    // `received_instant` (M1), not the wall-clock `receivetime`.
    p.received_instant = Instant::now()
        .checked_sub(std::time::Duration::from_secs(60))
        .expect("system clock too close to boot for this test");
    assert_eq!(droppacket(&p), Some(DropReason::Stale));
}

#[test]
fn test_fresh_packet_not_dropped() {
    let p = make_packet();
    assert!(droppacket(&p).is_none());
}


// ========================================
// positpacket tests
// ========================================

#[test]
fn test_positpacket_valid() {
    let location = Location { lat: Some(39.7392), lon: Some(-104.9903), alt: Some(5280.0), source: PositionSource::Config };
    let result = positpacket(&location, "N0CALL", "Test", &Some("\\&".to_string()), &Some("R".to_string()), DaoMode::Human);
    assert!(result.is_ok());
    let pkt = result.unwrap();
    assert!(pkt.starts_with("N0CALL>APZJD1,TCPIP*:/"));
    assert!(pkt.contains("/A="));
    assert!(pkt.contains("Test"));
}

#[test]
fn test_positpacket_missing_lat() {
    let location = Location { lat: None, lon: Some(-104.0), alt: Some(5000.0), source: PositionSource::Config };
    let result = positpacket(&location, "N0CALL", "Test", &None, &None, DaoMode::Human);
    assert!(result.is_err());
}

#[test]
fn test_positpacket_zero_alt() {
    let location = Location { lat: Some(39.0), lon: Some(-104.0), alt: Some(0.0), source: PositionSource::Config };
    let result = positpacket(&location, "N0CALL", "Test", &None, &None, DaoMode::Human);
    assert!(result.is_err());
}

#[test]
fn test_positpacket_default_symbol() {
    let location = Location { lat: Some(39.0), lon: Some(-104.0), alt: Some(5000.0), source: PositionSource::Config };
    let result = positpacket(&location, "N0CALL", "Test", &None, &None, DaoMode::Human);
    assert!(result.is_ok());
}

#[test]
fn test_positpacket_southern_hemisphere() {
    let location = Location { lat: Some(-33.8688), lon: Some(151.2093), alt: Some(100.0), source: PositionSource::Config };
    let result = positpacket(&location, "VK2ABC", "Sydney", &None, &None, DaoMode::Human);
    assert!(result.is_ok());
    let pkt = result.unwrap();
    assert!(pkt.contains("S"));
    assert!(pkt.contains("E"));
}


// ========================================
// Telemetry encoding tests
// ========================================

#[test]
fn test_telemetry_to_aprs_basic() {
    let items = vec![
        AnalogItem { label: "Rx".into(), units: "Pkts".into(), equation: APRSQuadratic::new(10.0) },
    ];
    let telem = Telemetry { telemetry: items, name: "Test".into(), sequence: 1 };
    let result = telem.to_aprs(&"N0CALL".to_string());
    assert!(result.is_ok());
    let packets = result.unwrap();
    assert_eq!(packets.len(), 5); // T#, EQNS, PARM, UNIT, BITS
    assert!(packets[0].starts_with("T#1,"));
    assert!(packets[1].contains("EQNS"));
    assert!(packets[2].contains("PARM"));
    assert!(packets[3].contains("UNIT"));
    assert!(packets[4].contains("BITS"));
}

#[test]
fn test_telemetry_empty_items_error() {
    let telem = Telemetry { telemetry: vec![], name: "Test".into(), sequence: 1 };
    let result = telem.to_aprs(&"N0CALL".to_string());
    assert!(result.is_err());
}

#[test]
fn test_telemetry_five_items_no_padding() {
    let items: Vec<AnalogItem> = (0..5).map(|i| {
        AnalogItem { label: format!("A{}", i), units: "U".into(), equation: APRSQuadratic::new(i as f64) }
    }).collect();
    let telem = Telemetry { telemetry: items, name: "Test".into(), sequence: 42 };
    let result = telem.to_aprs(&"N0CALL".to_string());
    assert!(result.is_ok());
    let packets = result.unwrap();
    assert!(packets[0].starts_with("T#42,"));
    // Should not contain padding zeros (already has 5 items)
}

#[test]
fn test_telemetry_max_six_items_truncates_to_five() {
    let items: Vec<AnalogItem> = (0..6).map(|i| {
        AnalogItem { label: format!("A{}", i), units: "U".into(), equation: APRSQuadratic::new(i as f64) }
    }).collect();
    let telem = Telemetry { telemetry: items, name: "Test".into(), sequence: 1 };
    let result = telem.to_aprs(&"N0CALL".to_string());
    assert!(result.is_ok());
    // The T# line should have exactly 5 analog values + digital + name
    let t_line = &result.unwrap()[0];
    let comma_count = t_line.matches(',').count();
    // T#seq,v1,v2,v3,v4,v5,00000000,name = 7 commas
    assert_eq!(comma_count, 7);
}


// ========================================
// APRSQuadratic tests
// ========================================

#[test]
fn test_quadratic_small_value() {
    let q = APRSQuadratic::new(42.0);
    // For small values: a=0, b=1, x=floor(value), c=remainder
    assert_eq!(q.a, 0.0);
    assert_eq!(q.b, 1.0);
    assert_eq!(q.x, 42);
}

#[test]
fn test_quadratic_zero() {
    let q = APRSQuadratic::new(0.0);
    assert_eq!(q.x, 0);
    assert_eq!(q.a, 0.0);
    assert_eq!(q.b, 1.0);
}

#[test]
fn test_quadratic_fractional() {
    let q = APRSQuadratic::new(42.5);
    assert_eq!(q.x, 42);
    assert_eq!(q.a, 0.0);
    assert_eq!(q.b, 1.0);
    assert!((q.c - 0.5).abs() < 0.001);
}


// ========================================
// RTPPacket::for_rxigate tests
// ========================================

#[test]
fn test_for_rxigate_format() {
    let p = make_packet();
    let result = p.for_rxigate("MYCALL");
    assert!(result.starts_with("N0CALL>APRS,WIDE1-1,qAO,MYCALL:"));
    assert!(result.contains("!4903.50N/07201.75W-"));
}

#[test]
fn test_for_rxigate_empty_path_no_double_comma() {
    // H1: direct-heard packets have an empty viapath. The output must not
    // contain a double comma — APRS-IS parsers treat that inconsistently.
    let mut p = make_packet();
    p.path = String::new();
    let result = p.for_rxigate("MYCALL");
    assert!(!result.contains(",,"), "double comma in {result}");
    assert!(result.starts_with("N0CALL>APRS,qAO,MYCALL:"));
}


// ========================================
// positpacket / DAO precision tests
// ========================================

#[test]
fn test_positpacket_includes_human_readable_dao() {
    // Live rpi5 coordinates: 39.348740067, -104.797664617
    // base ddmm.hh = 3920.92N / 10447.85W, with thousandths digits 4 and 9 -> !W49!
    let loc = Location { lat: Some(39.348740067), lon: Some(-104.797664617), alt: Some(6655.0), source: PositionSource::Config };
    let p = positpacket(&loc, "N0CALL", "", &Some("\\&".to_string()), &Some("R".to_string()), DaoMode::Human).unwrap();
    assert!(p.contains("3920.92N"), "base latitude wrong: {}", p);
    assert!(p.contains("10447.85W"), "base longitude wrong: {}", p);
    assert!(p.contains("!W49!"), "DAO token missing/wrong: {}", p);
}

#[test]
fn test_positpacket_dao_zero_with_no_extra_precision() {
    // Exactly representable: 39.5 -> 30.000', -104.25 -> 15.000' -> !W00!
    let loc = Location { lat: Some(39.5), lon: Some(-104.25), alt: Some(5280.0), source: PositionSource::Config };
    let p = positpacket(&loc, "N0CALL", "", &Some("\\&".to_string()), &Some("R".to_string()), DaoMode::Human).unwrap();
    assert!(p.contains("3930.00N"), "{}", p);
    assert!(p.contains("10415.00W"), "{}", p);
    assert!(p.contains("!W00!"), "{}", p);
}

#[test]
fn test_positpacket_base91_dao() {
    // Same live coordinates, base-91 form. lat remainder 0.004404' -> 'I' (val 40),
    // lon remainder 0.009877' -> 'z' (val 89), lowercase 'w' datum -> !wIz!
    let loc = Location { lat: Some(39.348740067), lon: Some(-104.797664617), alt: Some(6655.0), source: PositionSource::Config };
    let p = positpacket(&loc, "N0CALL", "", &Some("\\&".to_string()), &Some("R".to_string()), DaoMode::Base91).unwrap();
    assert!(p.contains("3920.92N"), "base latitude wrong: {}", p);
    assert!(p.contains("10447.85W"), "base longitude wrong: {}", p);
    assert!(p.contains("!wIz!"), "base-91 DAO token missing/wrong: {}", p);
}

#[test]
fn test_positpacket_dao_off_emits_no_token() {
    let loc = Location { lat: Some(39.348740067), lon: Some(-104.797664617), alt: Some(6655.0), source: PositionSource::Config };
    let p = positpacket(&loc, "N0CALL", "", &Some("\\&".to_string()), &Some("R".to_string()), DaoMode::Off).unwrap();
    assert!(p.contains("3920.92N"), "{}", p);
    assert!(!p.contains("!W") && !p.contains("!w"), "unexpected DAO token: {}", p);
}
