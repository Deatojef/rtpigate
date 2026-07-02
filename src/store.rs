// Persistent statistics store, backed by native_db (redb under the hood).
//
// rtpigate keeps its statistics in RAM for fast reads and the same rolling-window
// eviction rules as before; this module mirrors that state to an on-disk database
// so a restart — planned or a crash — resumes with the statistics intact. Each
// task snapshots its state on its existing ~15s telemetry tick and once more on
// shutdown, so a crash loses at most one tick.
//
// The on-disk schema is decoupled from the SSE/telemetry structs: dedicated
// `*Rec` record types carry native_db's model attributes and convert to/from the
// runtime types (`StationEntry`, `StatBucket`, `RTPPacket`, …). This keeps the
// database's versioned models independent of the JSON payloads sent to browsers.
//
// A single `Store` (wrapped in an `Arc`) is shared across the rtp_listener,
// aprsis, and sse tasks. native_db serializes writers via transactions, so no
// extra locking is needed.

use std::sync::LazyLock;
use std::time::Instant;

use chrono::{DateTime, Local, Utc};
use native_db::*;
use native_model::{Model, native_model};
use serde::{Deserialize, Serialize};

use crate::config::{SlicerInterval, StationEntry};
use crate::error::RtpigateError;
use crate::history::StatBucket;
use crate::stream::RTPPacket;

// Primary key for every singleton record (one row per table).
const SINGLETON: u8 = 0;

//======================= record types ===============================

// Singleton: lifetime packet counters owned by rtp_listener.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[native_model(id = 1, version = 1)]
#[native_db]
pub struct PacketLifetime {
    #[primary_key]
    pub id: u8,
    pub total: u64,
    pub heard_direct: u64,
    pub digipeated: u64,
    pub decode_errors: u64,
}

// Singleton: lifetime APRS-IS counters owned by aprsis_task.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[native_model(id = 2, version = 1)]
#[native_db]
pub struct AprsisLifetime {
    #[primary_key]
    pub id: u8,
    pub rf_received: u64,
    pub packets_igated: u64,
    pub packets_dropped: u64,
    pub reconnects: u64,
    pub drops_stale: u64,
    pub drops_rfonly: u64,
    pub drops_query: u64,
    pub drops_thirdparty: u64,
    pub drops_sat: u64,
    pub drops_duplicate: u64,
    pub drops_malformed: u64,
    pub lagged_drops: u64,
}

// One 15s heatmap row inside the slicer singleton (embedded, not its own table).
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SlicerIntervalRec {
    ts_micros: i64,
    counts: Vec<u32>,
}

// Singleton: slicer geometry + lifetime hits + the last-10 heatmap window.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[native_model(id = 3, version = 1)]
#[native_db]
struct SlicerState {
    #[primary_key]
    id: u8,
    slicer_count: u64,
    slicer_gains: Vec<f32>,
    lifetime_hits: Vec<u64>,
    history: Vec<SlicerIntervalRec>,
}

/// The persistent slicer accumulators restored at startup. Geometry (slicer
/// count / gain ladder) is intentionally *not* restored — the rtp_listener learns
/// it fresh from the wire — so these are seeded only when their length matches the
/// live slicer bank.
pub struct RestoredSlicer {
    pub lifetime_hits: Vec<u64>,
    pub history: Vec<SlicerInterval>,
}

// Singleton: the APRS telemetry sequence number (migrated off /tmp/telem-seq.txt).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[native_model(id = 4, version = 1)]
#[native_db]
pub struct TelemetrySeq {
    #[primary_key]
    id: u8,
    pub seq: u32,
}

// Table keyed by callsign: one heard station.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[native_model(id = 5, version = 1)]
#[native_db]
struct StationRec {
    #[primary_key]
    callsign: String,
    transmitted_by: Option<String>,
    last_heard_micros: i64,
    frequency: f64,
    latitude: Option<f64>,
    longitude: Option<f64>,
    altitude_ft: Option<f64>,
    heard_direct: bool,
    position_path: Vec<String>,
    position_hops: u32,
    altitude_path: Vec<String>,
    altitude_hops: u32,
    symbol_table: Option<char>,
    symbol_code: Option<char>,
    count: u64,
    count_direct: u64,
    count_digipeated: u64,
}

// Table keyed by frequency string: a per-frequency heard count + last-heard time.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[native_model(id = 6, version = 1)]
#[native_db]
struct FreqRec {
    #[primary_key]
    frequency: String,
    count: u64,
    last_heard_micros: i64,
}

// Table keyed by 15s-floored epoch second: one merged history bucket.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[native_model(id = 7, version = 1)]
#[native_db]
struct HistoryBucketRec {
    #[primary_key]
    ts: i64,
    total: u32,
    direct: u32,
    digipeated: u32,
    errors: u32,
    igated: u32,
    dropped: u32,
    rf_received: u32,
    reconnects: u32,
}

// Table keyed by receive-time microseconds: one satellite packet (display fields
// only; the non-serialized RTPPacket fields are reconstructed with defaults).
#[derive(Serialize, Deserialize, Debug, Clone)]
#[native_model(id = 8, version = 1)]
#[native_db]
struct SatPacketRec {
    #[primary_key]
    key_micros: i64,
    raw: String,
    info: String,
    path: String,
    digipeater_path: Vec<String>,
    hops: u32,
    ptype: char,
    source: String,
    destination: String,
    heard_direct: bool,
    heardfrom: String,
    was_digipeated: bool,
    rfonly: bool,
    frequency: f64,
    is_satellite: bool,
    igated: bool,
    info_invalid_bytes: u64,
    object_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    altitude_ft: Option<f64>,
}

//======================= model registry ===============================

static MODELS: LazyLock<Models> = LazyLock::new(|| {
    let mut models = Models::new();
    // These defines only fail on a duplicate model id — a programming error — so
    // panicking here (at first use) is the right response.
    models.define::<PacketLifetime>().unwrap();
    models.define::<AprsisLifetime>().unwrap();
    models.define::<SlicerState>().unwrap();
    models.define::<TelemetrySeq>().unwrap();
    models.define::<StationRec>().unwrap();
    models.define::<FreqRec>().unwrap();
    models.define::<HistoryBucketRec>().unwrap();
    models.define::<SatPacketRec>().unwrap();
    models
});

//======================= time helpers ===============================

fn to_micros(dt: &DateTime<Local>) -> i64 {
    dt.timestamp_micros()
}

fn from_micros(micros: i64) -> DateTime<Local> {
    DateTime::<Utc>::from_timestamp_micros(micros)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
        .with_timezone(&Local)
}

//======================= conversions ===============================

impl StationRec {
    fn from_entry(s: &StationEntry) -> Self {
        StationRec {
            callsign: s.callsign.clone(),
            transmitted_by: s.transmitted_by.clone(),
            last_heard_micros: to_micros(&s.last_heard),
            frequency: s.frequency,
            latitude: s.latitude,
            longitude: s.longitude,
            altitude_ft: s.altitude_ft,
            heard_direct: s.heard_direct,
            position_path: s.position_path.clone(),
            position_hops: s.position_hops,
            altitude_path: s.altitude_path.clone(),
            altitude_hops: s.altitude_hops,
            symbol_table: s.symbol_table,
            symbol_code: s.symbol_code,
            count: s.count,
            count_direct: s.count_direct,
            count_digipeated: s.count_digipeated,
        }
    }

    fn into_entry(self) -> StationEntry {
        StationEntry {
            callsign: self.callsign,
            transmitted_by: self.transmitted_by,
            last_heard: from_micros(self.last_heard_micros),
            frequency: self.frequency,
            latitude: self.latitude,
            longitude: self.longitude,
            altitude_ft: self.altitude_ft,
            heard_direct: self.heard_direct,
            position_path: self.position_path,
            position_hops: self.position_hops,
            altitude_path: self.altitude_path,
            altitude_hops: self.altitude_hops,
            symbol_table: self.symbol_table,
            symbol_code: self.symbol_code,
            count: self.count,
            count_direct: self.count_direct,
            count_digipeated: self.count_digipeated,
        }
    }
}

impl HistoryBucketRec {
    fn from_bucket(b: &StatBucket) -> Self {
        HistoryBucketRec {
            ts: b.ts,
            total: b.total,
            direct: b.direct,
            digipeated: b.digipeated,
            errors: b.errors,
            igated: b.igated,
            dropped: b.dropped,
            rf_received: b.rf_received,
            reconnects: b.reconnects,
        }
    }

    fn into_bucket(self) -> StatBucket {
        StatBucket {
            ts: self.ts,
            total: self.total,
            direct: self.direct,
            digipeated: self.digipeated,
            errors: self.errors,
            igated: self.igated,
            dropped: self.dropped,
            rf_received: self.rf_received,
            reconnects: self.reconnects,
        }
    }
}

impl SatPacketRec {
    fn from_packet(p: &RTPPacket) -> Self {
        SatPacketRec {
            key_micros: to_micros(&p.receivetime),
            raw: p.raw.clone(),
            info: p.info.clone(),
            path: p.path.clone(),
            digipeater_path: p.digipeater_path.clone(),
            hops: p.hops,
            ptype: p.ptype,
            source: p.source.clone(),
            destination: p.destination.clone(),
            heard_direct: p.heard_direct,
            heardfrom: p.heardfrom.clone(),
            was_digipeated: p.was_digipeated,
            rfonly: p.rfonly,
            frequency: p.frequency,
            is_satellite: p.is_satellite,
            igated: p.igated,
            info_invalid_bytes: p.info_invalid_bytes as u64,
            object_name: p.object_name.clone(),
            latitude: p.latitude,
            longitude: p.longitude,
            altitude_ft: p.altitude_ft,
        }
    }

    // Reconstruct a display-only RTPPacket. The three non-serialized fields
    // (`received_instant`, `info_bytes`, `slicer_mask`) and `twist` are only used
    // in the live gating/aggregation paths, never for restored packets (which are
    // read solely by the /api/satellite-packets JSON handler), so defaults are safe.
    fn into_packet(self) -> RTPPacket {
        RTPPacket {
            receivetime: from_micros(self.key_micros),
            received_instant: Instant::now(),
            raw: self.raw,
            info: self.info,
            info_bytes: Vec::new(),
            path: self.path,
            digipeater_path: self.digipeater_path,
            hops: self.hops,
            ptype: self.ptype,
            source: self.source,
            destination: self.destination,
            heard_direct: self.heard_direct,
            heardfrom: self.heardfrom,
            was_digipeated: self.was_digipeated,
            rfonly: self.rfonly,
            frequency: self.frequency,
            is_satellite: self.is_satellite,
            igated: self.igated,
            info_invalid_bytes: self.info_invalid_bytes as usize,
            object_name: self.object_name,
            latitude: self.latitude,
            longitude: self.longitude,
            altitude_ft: self.altitude_ft,
            slicer_mask: 0,
            twist: None,
        }
    }
}

//======================= the store ===============================

pub struct Store {
    db: Database<'static>,
}

impl Store {
    /// Open (creating if absent) the statistics database at `path`.
    pub fn open(path: &str) -> Result<Self, RtpigateError> {
        let db = Builder::new().create(&MODELS, path)?;
        Ok(Store { db })
    }

    //---- singletons -------------------------------------------------

    pub fn load_packet_lifetime(&self) -> Result<PacketLifetime, RtpigateError> {
        let r = self.db.r_transaction()?;
        Ok(r.get().primary(SINGLETON)?.unwrap_or_default())
    }

    pub fn save_packet_lifetime(&self, v: &PacketLifetime) -> Result<(), RtpigateError> {
        let mut v = v.clone();
        v.id = SINGLETON;
        let rw = self.db.rw_transaction()?;
        rw.upsert(v)?;
        rw.commit()?;
        Ok(())
    }

    pub fn load_aprsis_lifetime(&self) -> Result<AprsisLifetime, RtpigateError> {
        let r = self.db.r_transaction()?;
        Ok(r.get().primary(SINGLETON)?.unwrap_or_default())
    }

    pub fn save_aprsis_lifetime(&self, v: &AprsisLifetime) -> Result<(), RtpigateError> {
        let mut v = v.clone();
        v.id = SINGLETON;
        let rw = self.db.rw_transaction()?;
        rw.upsert(v)?;
        rw.commit()?;
        Ok(())
    }

    /// Returns the persisted slicer accumulators, or `None` when nothing has been
    /// stored yet (so the caller keeps learning geometry lazily from the wire).
    pub fn load_slicer(&self) -> Result<Option<RestoredSlicer>, RtpigateError> {
        let r = self.db.r_transaction()?;
        let rec: Option<SlicerState> = r.get().primary(SINGLETON)?;
        Ok(rec.map(|s| RestoredSlicer {
            lifetime_hits: s.lifetime_hits,
            history: s
                .history
                .into_iter()
                .map(|h| SlicerInterval {
                    timestamp: from_micros(h.ts_micros),
                    counts: h.counts,
                })
                .collect(),
        }))
    }

    pub fn save_slicer(
        &self,
        slicer_count: usize,
        gains: &[f32],
        lifetime_hits: &[u64],
        history: &[SlicerInterval],
    ) -> Result<(), RtpigateError> {
        let rec = SlicerState {
            id: SINGLETON,
            slicer_count: slicer_count as u64,
            slicer_gains: gains.to_vec(),
            lifetime_hits: lifetime_hits.to_vec(),
            history: history
                .iter()
                .map(|h| SlicerIntervalRec {
                    ts_micros: to_micros(&h.timestamp),
                    counts: h.counts.clone(),
                })
                .collect(),
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(rec)?;
        rw.commit()?;
        Ok(())
    }

    pub fn load_telemetry_seq(&self) -> Result<Option<u32>, RtpigateError> {
        let r = self.db.r_transaction()?;
        let rec: Option<TelemetrySeq> = r.get().primary(SINGLETON)?;
        Ok(rec.map(|t| t.seq))
    }

    pub fn save_telemetry_seq(&self, seq: u32) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        rw.upsert(TelemetrySeq {
            id: SINGLETON,
            seq,
        })?;
        rw.commit()?;
        Ok(())
    }

    //---- stations ---------------------------------------------------

    pub fn load_stations(&self) -> Result<Vec<StationEntry>, RtpigateError> {
        let r = self.db.r_transaction()?;
        let recs: Vec<StationRec> = r.scan().primary()?.all()?.collect::<Result<_, _>>()?;
        Ok(recs.into_iter().map(StationRec::into_entry).collect())
    }

    pub fn upsert_station(&self, s: &StationEntry) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        rw.upsert(StationRec::from_entry(s))?;
        rw.commit()?;
        Ok(())
    }

    pub fn delete_station(&self, callsign: &str) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        if let Some(rec) = rw.get().primary::<StationRec>(callsign.to_string())? {
            rw.remove(rec)?;
        }
        rw.commit()?;
        Ok(())
    }

    //---- frequency counts ------------------------------------------

    pub fn load_freqs(&self) -> Result<Vec<(String, u64, DateTime<Local>)>, RtpigateError> {
        let r = self.db.r_transaction()?;
        let recs: Vec<FreqRec> = r.scan().primary()?.all()?.collect::<Result<_, _>>()?;
        Ok(recs
            .into_iter()
            .map(|f| (f.frequency, f.count, from_micros(f.last_heard_micros)))
            .collect())
    }

    pub fn upsert_freq(
        &self,
        frequency: &str,
        count: u64,
        last_heard: &DateTime<Local>,
    ) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        rw.upsert(FreqRec {
            frequency: frequency.to_string(),
            count,
            last_heard_micros: to_micros(last_heard),
        })?;
        rw.commit()?;
        Ok(())
    }

    pub fn delete_freq(&self, frequency: &str) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        if let Some(rec) = rw.get().primary::<FreqRec>(frequency.to_string())? {
            rw.remove(rec)?;
        }
        rw.commit()?;
        Ok(())
    }

    //---- history buckets -------------------------------------------

    pub fn load_buckets(&self) -> Result<Vec<StatBucket>, RtpigateError> {
        let r = self.db.r_transaction()?;
        let recs: Vec<HistoryBucketRec> = r.scan().primary()?.all()?.collect::<Result<_, _>>()?;
        Ok(recs.into_iter().map(HistoryBucketRec::into_bucket).collect())
    }

    /// Upsert a batch of buckets in a single transaction.
    pub fn upsert_buckets(&self, buckets: &[StatBucket]) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        for b in buckets {
            rw.upsert(HistoryBucketRec::from_bucket(b))?;
        }
        rw.commit()?;
        Ok(())
    }

    /// Delete buckets by key (the ts values dropped by the in-memory prune).
    pub fn delete_buckets(&self, keys: &[i64]) -> Result<(), RtpigateError> {
        if keys.is_empty() {
            return Ok(());
        }
        let rw = self.db.rw_transaction()?;
        for &ts in keys {
            if let Some(rec) = rw.get().primary::<HistoryBucketRec>(ts)? {
                rw.remove(rec)?;
            }
        }
        rw.commit()?;
        Ok(())
    }

    //---- satellite packet log --------------------------------------

    pub fn load_sat_packets(&self) -> Result<Vec<RTPPacket>, RtpigateError> {
        let r = self.db.r_transaction()?;
        // Primary-key order is ascending receive time — oldest-first, matching the
        // in-memory VecDeque's push_back ordering.
        let recs: Vec<SatPacketRec> = r.scan().primary()?.all()?.collect::<Result<_, _>>()?;
        Ok(recs.into_iter().map(SatPacketRec::into_packet).collect())
    }

    pub fn insert_sat_packet(&self, p: &RTPPacket) -> Result<(), RtpigateError> {
        let rw = self.db.rw_transaction()?;
        rw.upsert(SatPacketRec::from_packet(p))?;
        rw.commit()?;
        Ok(())
    }

    pub fn delete_sat_packets(&self, keys: &[i64]) -> Result<(), RtpigateError> {
        if keys.is_empty() {
            return Ok(());
        }
        let rw = self.db.rw_transaction()?;
        for &k in keys {
            if let Some(rec) = rw.get().primary::<SatPacketRec>(k)? {
                rw.remove(rec)?;
            }
        }
        rw.commit()?;
        Ok(())
    }
}

// The `key_micros` of a satellite packet, for computing prune deletions.
pub fn sat_packet_key(p: &RTPPacket) -> i64 {
    to_micros(&p.receivetime)
}
