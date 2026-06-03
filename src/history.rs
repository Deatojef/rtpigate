// Rolling 24-hour history of per-interval packet/igating statistics, merged from
// the two independently-ticking telemetry streams:
//
//   * `PacketTelemetry`  (ka9q rtp_listener)  — total / direct / digipeated / errors
//   * `AprsisTelemetry`  (aprs_is task)       — igated / dropped / rf_received / reconnects
//
// Both tasks emit on their own ~15s timer, so we merge by a 15s-aligned timestamp
// key: each event upserts only the fields it owns into the matching bucket. The
// store is written from `sse_task` (which already sees every telemetry tick) and
// read by the `/api/history` HTTP handler, mirroring the `sat_packet_log` pattern.

use std::collections::BTreeMap;

use chrono::Local;
use serde::Serialize;

use crate::config::{AprsisTelemetry, DataPoint, DataSeries, PacketTelemetry};

/// Bucket granularity in seconds — matches the telemetry tick cadence.
const BUCKET_SECS: i64 = 15;

/// Retention window: 24 hours.
const RETENTION_SECS: i64 = 24 * 60 * 60;

/// One 15-second bucket of merged statistics. `ts` is the bucket's epoch second,
/// floored to a `BUCKET_SECS` boundary. Count fields are per-interval (not
/// cumulative); the frontend aggregates them into wider display buckets and
/// derives the igated percentage as `igated / rf_received`.
#[derive(Serialize, Debug, Clone, Default, PartialEq)]
pub struct StatBucket {
    pub ts: i64,
    pub total: u32,
    pub direct: u32,
    pub digipeated: u32,
    pub errors: u32,
    pub igated: u32,
    pub dropped: u32,
    pub rf_received: u32,
    pub reconnects: u32,
}

/// 24-hour rolling store of `StatBucket`s keyed by floored epoch second.
#[derive(Debug, Default)]
pub struct HistoryStore {
    buckets: BTreeMap<i64, StatBucket>,
}

impl HistoryStore {
    pub fn new() -> Self {
        Self { buckets: BTreeMap::new() }
    }

    /// Floor an epoch second to the bucket boundary.
    fn bucket_key(epoch_secs: i64) -> i64 {
        epoch_secs - epoch_secs.rem_euclid(BUCKET_SECS)
    }

    /// Upsert every point of a `DataSeries` into its matching bucket using
    /// `set` to write the owned field. Iterating the whole series (rather than
    /// just the newest point) self-heals any tick we missed while no telemetry
    /// was flowing.
    fn merge_series<F>(&mut self, series: &DataSeries<u32>, set: F)
    where
        F: Fn(&mut StatBucket, u32),
    {
        for DataPoint { timestamp, value } in &series.data {
            let key = Self::bucket_key(timestamp.timestamp());
            let bucket = self.buckets.entry(key).or_insert_with(|| StatBucket { ts: key, ..Default::default() });
            set(bucket, *value);
        }
    }

    /// Merge the ka9q-side counts (total / direct / digipeated / errors).
    pub fn update_from_packet(&mut self, t: &PacketTelemetry) {
        self.merge_series(&t.total_packets, |b, v| b.total = v);
        self.merge_series(&t.heard_direct, |b, v| b.direct = v);
        self.merge_series(&t.digipeated, |b, v| b.digipeated = v);
        self.merge_series(&t.decode_errors, |b, v| b.errors = v);
    }

    /// Merge the APRS-IS-side counts (igated / dropped / rf_received / reconnects).
    pub fn update_from_aprsis(&mut self, t: &AprsisTelemetry) {
        self.merge_series(&t.packets_igated, |b, v| b.igated = v);
        self.merge_series(&t.packets_dropped, |b, v| b.dropped = v);
        self.merge_series(&t.rf_received, |b, v| b.rf_received = v);
        self.merge_series(&t.reconnects, |b, v| b.reconnects = v);
    }

    /// Drop buckets older than the retention window.
    pub fn prune(&mut self) {
        let cutoff = Self::bucket_key(Local::now().timestamp()) - RETENTION_SECS;
        // Retain only buckets at or after the cutoff.
        self.buckets.retain(|&ts, _| ts >= cutoff);
    }

    /// Oldest-first snapshot of all buckets, for the `/api/history` endpoint.
    pub fn snapshot(&self) -> Vec<StatBucket> {
        self.buckets.values().cloned().collect()
    }
}
