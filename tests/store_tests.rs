// Round-trip tests for the persistent statistics store (native_db). These verify
// the record <-> runtime-type conversions and the load/upsert/delete helpers that
// the offline app run can't exercise without a live stream / APRS-IS connection.

use std::time::Instant;

use chrono::Local;

use rtpigate::config::{SlicerInterval, StationEntry};
use rtpigate::history::StatBucket;
use rtpigate::store::{PacketLifetime, Store};
use rtpigate::stream::RTPPacket;

fn temp_db_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("rtpigate_store_test_{}_{}.db", tag, nanos))
        .to_string_lossy()
        .into_owned()
}

fn sample_station(callsign: &str, count: u64) -> StationEntry {
    StationEntry {
        callsign: callsign.to_string(),
        transmitted_by: None,
        last_heard: Local::now(),
        frequency: 144.390,
        latitude: Some(40.1),
        longitude: Some(-105.2),
        altitude_ft: Some(1234.0),
        heard_direct: true,
        position_path: vec!["WIDE1-1".to_string()],
        position_hops: 1,
        altitude_path: vec!["WIDE1-1".to_string()],
        altitude_hops: 1,
        symbol_table: Some('/'),
        symbol_code: Some('>'),
        count,
        count_direct: count,
        count_digipeated: 0,
    }
}

fn sample_packet(freq: f64) -> RTPPacket {
    RTPPacket {
        receivetime: Local::now(),
        received_instant: Instant::now(),
        raw: "N0CALL>APRS:>test".to_string(),
        info: ">test".to_string(),
        info_bytes: Vec::new(),
        path: String::new(),
        digipeater_path: vec![],
        hops: 0,
        ptype: '>',
        source: "N0CALL".to_string(),
        destination: "APRS".to_string(),
        heard_direct: true,
        heardfrom: "N0CALL".to_string(),
        was_digipeated: false,
        rfonly: false,
        frequency: freq,
        is_satellite: true,
        igated: false,
        info_invalid_bytes: 0,
        object_name: None,
        latitude: Some(40.0),
        longitude: Some(-105.0),
        altitude_ft: None,
        slicer_mask: 0,
        twist: None,
    }
}

#[test]
fn packet_lifetime_round_trips() {
    let path = temp_db_path("lifetime");
    let store = Store::open(&path).unwrap();

    // default when empty
    assert_eq!(store.load_packet_lifetime().unwrap().total, 0);

    store
        .save_packet_lifetime(&PacketLifetime {
            id: 0,
            total: 42,
            heard_direct: 30,
            digipeated: 12,
            decode_errors: 0,
        })
        .unwrap();

    let loaded = store.load_packet_lifetime().unwrap();
    assert_eq!(loaded.total, 42);
    assert_eq!(loaded.heard_direct, 30);
    assert_eq!(loaded.digipeated, 12);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn stations_upsert_load_delete() {
    let path = temp_db_path("stations");
    let store = Store::open(&path).unwrap();

    store.upsert_station(&sample_station("N0CALL", 5)).unwrap();
    store.upsert_station(&sample_station("W1AW", 9)).unwrap();

    let mut loaded = store.load_stations().unwrap();
    loaded.sort_by(|a, b| a.callsign.cmp(&b.callsign));
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].callsign, "N0CALL");
    assert_eq!(loaded[0].count, 5);
    assert_eq!(loaded[0].symbol_code, Some('>'));
    assert_eq!(loaded[1].callsign, "W1AW");

    // upsert replaces, not duplicates
    store.upsert_station(&sample_station("N0CALL", 6)).unwrap();
    assert_eq!(store.load_stations().unwrap().len(), 2);

    store.delete_station("N0CALL").unwrap();
    let after = store.load_stations().unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].callsign, "W1AW");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn frequencies_round_trip() {
    let path = temp_db_path("freqs");
    let store = Store::open(&path).unwrap();
    let now = Local::now();

    store.upsert_freq("144.390", 7, &now).unwrap();
    store.upsert_freq("145.825", 3, &now).unwrap();

    let loaded = store.load_freqs().unwrap();
    assert_eq!(loaded.len(), 2);

    store.delete_freq("144.390").unwrap();
    assert_eq!(store.load_freqs().unwrap().len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn history_buckets_upsert_and_delete() {
    let path = temp_db_path("buckets");
    let store = Store::open(&path).unwrap();

    let buckets = vec![
        StatBucket {
            ts: 1000,
            total: 5,
            rf_received: 5,
            igated: 4,
            ..Default::default()
        },
        StatBucket {
            ts: 1015,
            total: 2,
            ..Default::default()
        },
    ];
    store.upsert_buckets(&buckets).unwrap();

    let mut loaded = store.load_buckets().unwrap();
    loaded.sort_by_key(|b| b.ts);
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].ts, 1000);
    assert_eq!(loaded[0].total, 5);
    assert_eq!(loaded[0].igated, 4);

    store.delete_buckets(&[1000]).unwrap();
    let after = store.load_buckets().unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].ts, 1015);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn slicer_round_trip() {
    let path = temp_db_path("slicer");
    let store = Store::open(&path).unwrap();

    assert!(store.load_slicer().unwrap().is_none());

    let history = vec![
        SlicerInterval {
            timestamp: Local::now(),
            counts: vec![1, 2, 3],
        },
        SlicerInterval {
            timestamp: Local::now(),
            counts: vec![4, 5, 6],
        },
    ];
    store
        .save_slicer(3, &[0.7, 1.0, 1.3], &[10, 20, 30], &history)
        .unwrap();

    let restored = store.load_slicer().unwrap().expect("slicer state present");
    assert_eq!(restored.lifetime_hits, vec![10, 20, 30]);
    assert_eq!(restored.history.len(), 2);
    assert_eq!(restored.history[1].counts, vec![4, 5, 6]);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn telemetry_seq_round_trip() {
    let path = temp_db_path("seq");
    let store = Store::open(&path).unwrap();

    assert_eq!(store.load_telemetry_seq().unwrap(), None);
    store.save_telemetry_seq(123).unwrap();
    assert_eq!(store.load_telemetry_seq().unwrap(), Some(123));
    let _ = std::fs::remove_file(&path);
}

fn packet_with(source: &str, receivetime: chrono::DateTime<Local>) -> RTPPacket {
    let mut p = sample_packet(144.390);
    p.source = source.to_string();
    p.receivetime = receivetime;
    p
}

#[test]
fn watched_stations_add_load_remove() {
    let path = temp_db_path("watched");
    let store = Store::open(&path).unwrap();

    assert!(store.load_watched_stations().unwrap().is_empty());

    // Add out of chronological order; load returns them oldest-added first.
    let now = Local::now();
    store
        .add_watched_station("W1AW-9", &(now + chrono::Duration::seconds(2)))
        .unwrap();
    store.add_watched_station("N0CALL-1", &now).unwrap();

    let loaded = store.load_watched_stations().unwrap();
    assert_eq!(loaded, vec!["N0CALL-1".to_string(), "W1AW-9".to_string()]);

    store.remove_watched_station("N0CALL-1").unwrap();
    assert_eq!(
        store.load_watched_stations().unwrap(),
        vec!["W1AW-9".to_string()]
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn packets_query_by_source_and_prune() {
    let path = temp_db_path("packets");
    let store = Store::open(&path).unwrap();

    // Distinct receive times: the primary key is receive-time micros, so packets
    // sharing a microsecond would collide (as with the satellite log).
    let now = Local::now();
    let old = now - chrono::Duration::hours(2);
    let mid = now - chrono::Duration::minutes(30);

    // Two packets from A (different times) + one from B.
    store.insert_packet(&packet_with("A-1", old)).unwrap();
    store.insert_packet(&packet_with("A-1", now)).unwrap();
    store.insert_packet(&packet_with("B-2", mid)).unwrap();

    // Secondary-key query returns only the requested source, newest-first.
    let a = store.load_packets_by_source("A-1").unwrap();
    assert_eq!(a.len(), 2);
    assert!(a.iter().all(|p| p.source == "A-1"));
    assert!(a[0].receivetime >= a[1].receivetime);
    assert_eq!(store.load_packets_by_source("B-2").unwrap().len(), 1);
    assert!(store.load_packets_by_source("Z-9").unwrap().is_empty());

    // Prune everything older than 1h: the 2h-old A packet goes, the rest stay.
    let cutoff = (now - chrono::Duration::hours(1)).timestamp_micros();
    store.prune_packets(cutoff).unwrap();

    let a_after = store.load_packets_by_source("A-1").unwrap();
    assert_eq!(a_after.len(), 1);
    assert_eq!(
        a_after[0].receivetime.timestamp_micros(),
        now.timestamp_micros()
    );
    assert_eq!(store.load_packets_by_source("B-2").unwrap().len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn sat_packets_round_trip_and_prune() {
    let path = temp_db_path("sat");
    let store = Store::open(&path).unwrap();

    let p1 = sample_packet(145.825);
    let p2 = sample_packet(435.300);
    let k1 = p1.receivetime.timestamp_micros();
    store.insert_sat_packet(&p1).unwrap();
    store.insert_sat_packet(&p2).unwrap();

    let loaded = store.load_sat_packets().unwrap();
    assert_eq!(loaded.len(), 2);
    // display fields survive the projection
    assert!(
        loaded
            .iter()
            .any(|p| p.frequency == 145.825 && p.is_satellite)
    );
    assert!(loaded.iter().any(|p| p.source == "N0CALL"));

    store.delete_sat_packets(&[k1]).unwrap();
    let after = store.load_sat_packets().unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].frequency, 435.300);
    let _ = std::fs::remove_file(&path);
}
