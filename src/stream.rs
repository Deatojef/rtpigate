use chrono::{DateTime, Local, Utc};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    net::Ipv4Addr,
    sync::Arc,
    time::Instant,
};
use tokio::{
    sync::broadcast,
    time::{Duration, interval, sleep},
};
use tokio_util::sync::CancellationToken;

use log::{debug, error, info, warn};

use aprs_decode::Extension;
use aprs_decode::packet::{AprsData, AprsPacket as DecodedPacket};
use aprs_stream::subscribe::{RecvError, SubscribeConfig};
use aprs_stream::{AprsFrame, Subscriber};

use crate::config::{
    AppTelemetry, Config, DataItem, DataPoint, DataSeries, FrequencyCount, PacketTelemetry,
    SlicerInterval, SlicerTelemetry, StationEntry, StationTelemetry,
};
use crate::error::RtpigateError;
use crate::store::{Store, sat_packet_key};

// Classify a slicer's space-gain into a twist zone, mirroring the frontend's
// slicerZone(): gain < 0.8 compensates a loud space (pre-emphasis), gain > 1.25
// compensates a loud mark (de-emphasis); in between is treated as flat.
// 0 = pre-emph, 1 = flat, 2 = de-emph. The gain ladder itself now arrives on the
// wire (RfMeta::slicer_gains) instead of being derived from a local decoder
// config — the producer (aprs-streamd) owns the demodulator.
fn slicer_zone(g: f32) -> u8 {
    if g < 0.8 {
        0
    } else if g < 1.25 {
        1
    } else {
        2
    }
}

// Knots -> statute mph (APRS course/speed is native knots).
const KNOTS_TO_MPH: f64 = 1.150_779;

// Pull course/speed out of a position/object/item data extension. Returns
// (speed_mph, course_deg); course 000 (unknown/not applicable) becomes None.
fn course_speed_from_extension(ext: Option<&Extension>) -> (Option<f64>, Option<u16>) {
    match ext {
        Some(Extension::DirectionSpeed {
            direction_degrees,
            speed_knots,
        }) => {
            let course = if *direction_degrees == 0 {
                None
            } else {
                Some(*direction_degrees)
            };
            (Some(*speed_knots as f64 * KNOTS_TO_MPH), course)
        }
        _ => (None, None),
    }
}

// Per-packet "twist" summary feeding the Recent Packets twist bar. Twist is the
// mark/space amplitude imbalance the frame arrived with; it's inferred from which
// slicers in the gain ladder decoded the frame (low-gain slicers favor loud-space
// / pre-emphasized signals, high-gain favor loud-mark / de-emphasized). The
// frontend draws `cols` cells, lights the ones set in `mask`, and colors each by
// `zones`. Computed backend-side because the gain ladder lives here, so the bar
// needs no slicer-telemetry to render.
#[derive(Debug, Clone, Serialize)]
pub struct TwistInfo {
    pub cols: usize,    // bar width = number of slicers in the bank
    pub mask: u16,      // bit i set = slicer i decoded this frame (a lit cell)
    pub zones: Vec<u8>, // per-slicer twist zone: 0 = pre-emph, 1 = flat, 2 = de-emph
    // Mean twist (dB) across the slicers that decoded this frame: 20*log10(gain).
    // Negative = space louder (pre-emph), positive = mark louder (de-emph), 0 =
    // flat. A human-readable point estimate of the frame's twist for the popup.
    pub centroid_db: f32,
}

// the packet structure (created by the stream listener for incoming frames)
#[derive(Debug, Clone, Serialize)]
pub struct RTPPacket {
    // when we initial received this packet over the network
    pub receivetime: DateTime<Local>,

    // monotonic clock reading at the same moment as `receivetime`. Used for
    // the staleness/age check so NTP corrections cannot spuriously age packets
    // out of the gating pipeline. Not serialised to SSE clients.
    #[serde(skip)]
    pub received_instant: Instant,

    // the packet itself
    pub raw: String,
    pub info: String,

    // Raw info-field bytes exactly as received, byte-for-byte. `info` above is a
    // lossy UTF-8 rendering suitable for display/dedup/SSE (browsers require valid
    // UTF-8); `info_bytes` preserves the original 8-bit payload so igating forwards
    // it verbatim instead of substituting U+FFFD replacement characters for bytes
    // like a stuck transmitter's trailing 0xff. APRS-IS is an 8-bit, line-delimited
    // byte stream, so this is the faithful representation to gate. Not serialised.
    //
    // Sourced from the stream frame by slicing the raw AX.25 at the producer's
    // `ax25_meta.info_offset`, so no AX.25 re-parsing happens consumer-side.
    #[serde(skip)]
    pub info_bytes: Vec<u8>,

    // viapath
    pub path: String,

    // filtered digipeater path (excludes WIDE*, TCPIP, etc.)
    pub digipeater_path: Vec<String>,
    pub hops: u32,

    // APRS data type
    pub ptype: char,

    // source and destination
    pub source: String,
    pub destination: String,

    // was this packet heard directly or from a digipeater.
    //
    // `heard_direct` follows the conventional "ignore fill-in digis" semantics:
    // a path of only WIDE1-1* still reports as direct. This matches how most
    // APRS UIs label packets, but it is *not* a strict "no asterisks anywhere"
    // check — for that, use `was_digipeated`.
    pub heard_direct: bool,
    pub heardfrom: String,

    // strict "any digipeater touched this packet" flag — true iff *any* address
    // in the path has its has-been-repeated bit set, including WIDE-class fill-ins.
    // Used by the satellite igate filter so a packet relayed by an unnamed
    // fill-in digi is correctly recognised as having been digipeated.
    pub was_digipeated: bool,

    // is this packet not to be igated and sent to the APRS-IS cloud?
    pub rfonly: bool,

    // the frequency in MHz (from the frame's RF metadata / SSRC)
    pub frequency: f64,

    // was this packet heard from or perhaps, destined to a satellite?
    pub is_satellite: bool,

    // whether this packet would be igated by droppacket() at receive time.
    // Mirrors what aprs_is.rs will decide, minus the dedup step — duplicates
    // within the gating window are still counted as "would-igate" here.
    pub igated: bool,

    // Count of info-field bytes the decoder flagged as almost certainly not real
    // APRS payload: C0 control bytes (other than tab/CR/LF) plus any invalid-UTF-8
    // bytes (e.g. a stuck transmitter's trailing 0xff). 0 for a clean frame; >0
    // means the displayed `info` is likely garbled. Advisory only — the frame is
    // still FCS-valid and the raw bytes are untouched. Carried on the wire in
    // `ax25_meta.info_invalid_bytes`.
    pub info_invalid_bytes: usize,

    // object or item name (if this packet is an object/item report)
    pub object_name: Option<String>,

    // parsed position data (if available)
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,

    // parsed course/speed (if the report carried a course-speed extension or is
    // Mic-E). Speed is converted to mph; course is degrees (None when unknown/000).
    pub speed_mph: Option<f64>,
    pub course_deg: Option<u16>,

    // bitmask of demodulator slicers that decoded this frame (bit i = slicer i).
    // Used only for the slicer-waterfall aggregation; not serialised per-packet.
    #[serde(skip)]
    pub slicer_mask: u16,

    // per-packet twist bar payload (RF only). Set in the listener loop where the
    // slicer gain ladder is in scope; None when no slicer data is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub twist: Option<TwistInfo>,
}

impl fmt::Display for RTPPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{0: <24} {1: <10} {2:.3}MHz direct: {3: <6} rfonly: {4: <6} {5:}",
            self.receivetime.format("%Y-%m-%d %H:%M:%S%.3f"),
            self.source,
            self.frequency,
            self.heard_direct,
            self.rfonly,
            self.raw,
        )
    }
}

impl RTPPacket {
    pub fn for_rxigate(&self, callsign: &str) -> String {
        // Direct-heard packets have an empty viapath. Including the empty path
        // would produce a double comma (SRC>DST,,qAO,...), which APRS-IS parsers
        // treat inconsistently, so omit the path element entirely in that case.
        if self.path.is_empty() {
            format!(
                "{}>{},qAO,{}:{}",
                self.source, self.destination, callsign, self.info
            )
        } else {
            format!(
                "{}>{},{},qAO,{}:{}",
                self.source, self.destination, self.path, callsign, self.info
            )
        }
    }

    /// Byte-faithful form of the igate line for writing to APRS-IS. Identical to
    /// `for_rxigate` for the (ASCII) header — source, destination, path and our
    /// callsign are all 7-bit AX.25 callsigns — but appends the raw info bytes
    /// verbatim rather than the lossy `info` String, so a packet's original 8-bit
    /// payload is gated unchanged instead of being mangled into U+FFFD sequences.
    /// The caller appends the `\r\n` line terminator.
    pub fn for_rxigate_bytes(&self, callsign: &str) -> Vec<u8> {
        let header = if self.path.is_empty() {
            format!("{}>{},qAO,{}:", self.source, self.destination, callsign)
        } else {
            format!(
                "{}>{},{},qAO,{}:",
                self.source, self.destination, self.path, callsign
            )
        };
        let mut out = header.into_bytes();
        out.extend_from_slice(&self.info_bytes);
        out
    }
}

// type of packets
// RTP is an intentional historical name retained through the aprs-stream refactor
// (cf. the `rtp_listener` task name); we don't rename it to `Rtp` here.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum Packet {
    RTP(RTPPacket),
}

// Constant slices for packet classification — no heap allocation per packet
const EXCLUDED_ADDRS: &[&str] = &["WIDE", "TCPIP", "NOGATE", "RFONLY", "SGATE"];
const RFONLY_ADDRS: &[&str] = &["TCPIP", "TCPXX", "RFONLY", "NOGATE"];

// Subscribes to the decoded-APRS multicast stream published by `aprs-streamd`
// (via the shared `aprs-stream` crate) and maps each typed frame into the
// internal RTPPacket type, broadcasting it on the shared data channel. All RTP
// audio, AFSK demodulation, HDLC/CRC and AX.25 parsing now happen once, upstream
// in the producer — this consumer never touches any of that.
//
// Normally this never returns — it loops forever, reconnecting on failure.
pub async fn rtp_listener(
    data_channel: broadcast::Sender<DataItem>,
    token: CancellationToken,
    config: Arc<Config>,
    sat_packet_log: Arc<std::sync::RwLock<VecDeque<RTPPacket>>>,
    store: Arc<Store>,
) -> Result<(), RtpigateError> {
    info!("Started");

    // satellite frequencies sourced from config (default [145.825] if unset)
    let sat_freqs = config.satellite_frequencies();

    // per-interval statistics
    let mut heard_direct = 0;
    let mut digipeated = 0;
    // `decode_errors` is retained for telemetry/frontend compatibility. The
    // producer only publishes successfully-decoded frames, so this counter is
    // never incremented and always reports 0.
    let mut decode_errors = 0;
    let mut total_packets = 0;

    // lifetime counters (never reset)
    let mut lifetime_total_packets: u64 = 0;
    let mut lifetime_heard_direct: u64 = 0;
    let mut lifetime_digipeated: u64 = 0;
    let lifetime_decode_errors: u64 = 0;

    // station tracking (never cleared)
    let mut station_map: HashMap<String, StationEntry> = HashMap::new();
    let mut freq_counts: HashMap<String, (u64, DateTime<Local>)> = HashMap::new();

    // data series
    let mut packets_series = DataSeries {
        name: String::from("total_packets"),
        data: VecDeque::new(),
    };
    let mut heard_direct_series = DataSeries {
        name: String::from("heard_direct"),
        data: VecDeque::new(),
    };
    let mut digipeated_series = DataSeries {
        name: String::from("digipeated"),
        data: VecDeque::new(),
    };
    let mut decode_errors_series = DataSeries {
        name: String::from("decode_errors"),
        data: VecDeque::new(),
    };

    // Slicer-diversity waterfall state. The slicer bank size and its per-slicer
    // gain ladder are no longer known up front (rtpigate no longer owns the
    // decoder) — they arrive on the wire in each frame's `RfMeta::slicer_gains`.
    // These are lazily initialized from the first frame that carries the ladder;
    // until then they stay empty and the slicer telemetry reports nothing.
    let mut slicer_count: usize = 0;
    let mut slicer_gains: Vec<f32> = Vec::new();
    // Twist zone per slicer (0 = pre-emph, 1 = flat, 2 = de-emph); cloned onto
    // each packet's TwistInfo so the frontend can color the twist bar.
    let mut slicer_zones: Vec<u8> = Vec::new();
    // Twist (dB) per slicer = 20*log10(gain); used to compute each packet's
    // centroid twist for the popup.
    let mut slicer_db: Vec<f32> = Vec::new();
    // per-slicer accumulators. `slicer_interval` counts demodulations in the
    // current 15s window; `slicer_history` keeps the last 10 windows (heatmap
    // rows); `lifetime_slicer_hits` never resets.
    let mut slicer_interval: Vec<u32> = Vec::new();
    let mut slicer_history: VecDeque<SlicerInterval> = VecDeque::new();
    let mut lifetime_slicer_hits: Vec<u64> = Vec::new();

    //#################
    // Seed the task-local statistics from the persistent store so a restart resumes
    // with prior counts. Geometry-free slicer accumulators are held aside and
    // applied once the wire reveals the (possibly changed) slicer bank size.
    //#################
    match store.load_packet_lifetime() {
        Ok(pl) => {
            lifetime_total_packets = pl.total;
            lifetime_heard_direct = pl.heard_direct;
            lifetime_digipeated = pl.digipeated;
        }
        Err(e) => warn!("Failed to load packet lifetime counters: {}", e),
    }
    match store.load_stations() {
        Ok(list) => {
            for s in list {
                station_map.insert(s.callsign.clone(), s);
            }
            info!("Restored {} station(s) from store", station_map.len());
        }
        Err(e) => warn!("Failed to load stations from store: {}", e),
    }
    match store.load_freqs() {
        Ok(list) => {
            for (freq, count, last_heard) in list {
                freq_counts.insert(freq, (count, last_heard));
            }
        }
        Err(e) => warn!("Failed to load frequency counts from store: {}", e),
    }
    let mut restored_slicer = match store.load_slicer() {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to load slicer state from store: {}", e);
            None
        }
    };

    // Keys mutated since the last persistence flush, so the tick upserts only the
    // stations/frequencies that actually changed rather than the whole map.
    let mut dirty_stations: HashSet<String> = HashSet::new();
    let mut dirty_freqs: HashSet<String> = HashSet::new();

    // the interval for when to send statistics
    let mut time_interval = interval(Duration::from_secs(15));

    // backoff state for reconnection
    let mut backoff_secs: u64 = 5;
    const MAX_BACKOFF_SECS: u64 = 300;

    // outer reconnection loop
    loop {
        if token.is_cancelled() {
            break;
        }

        // Join the multicast group and start receiving decoded frames. The
        // `[stream]` section points at the group/port `aprs-streamd` publishes to.
        // On setup failure, back off and retry exactly as the RTP path used to.
        let sub = match Subscriber::new(SubscribeConfig {
            group: config.stream.group,
            port: config.stream.port,
            interface: config.stream.interface.unwrap_or(Ipv4Addr::UNSPECIFIED),
            recv_buffer_bytes: config.stream.recv_buffer_bytes,
        }) {
            Ok(sub) => {
                backoff_secs = 5;
                sub
            }
            Err(e) => {
                error!(
                    "APRS stream subscribe failed: {}. Retrying in {}s...",
                    e, backoff_secs
                );
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
        };

        info!(
            "Subscribed to APRS stream {}:{}",
            config.stream.group, config.stream.port
        );

        // inner frame read loop
        loop {
            tokio::select! {

                // was this thread canceled?
                _ = token.cancelled() => {
                    break;
                },


                // send statistics
                _ = time_interval.tick() => {
                    let the_time = Local::now();

                    // add data points to series
                    packets_series.data.push_back(DataPoint { timestamp: the_time, value: total_packets, });
                    heard_direct_series.data.push_back(DataPoint { timestamp: the_time, value: heard_direct, });
                    digipeated_series.data.push_back(DataPoint { timestamp: the_time, value: digipeated, });
                    decode_errors_series.data.push_back(DataPoint { timestamp: the_time, value: decode_errors, });

                    // trim series to max 100 data points
                    for series in [&mut packets_series, &mut heard_direct_series, &mut digipeated_series, &mut decode_errors_series] {
                        if series.data.len() > 100 {
                            series.data.pop_front();
                        }
                    }

                    // get the current time
                    let now = Utc::now();

                    // and in microsecs since the epoch
                    let microsecs: f64 = now.timestamp_micros() as f64 / 1000000.0;

                    // create a new struct to hold data statistics
                    let data = PacketTelemetry {
                        name: String::from("packet_statistics"),
                        timestamp: the_time,
                        microsecs,
                        total_packets: packets_series.clone(),
                        heard_direct: heard_direct_series.clone(),
                        digipeated: digipeated_series.clone(),
                        decode_errors: decode_errors_series.clone(),
                        lifetime_total_packets,
                        lifetime_heard_direct,
                        lifetime_digipeated,
                        lifetime_decode_errors,
                    };

                    // Send statistics to the channel
                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::PacketStatus(data))) {
                        warn!("Failed to send statistics data to channel: {}", e);
                    }

                    // snapshot this window's per-slicer counts and keep the last 10
                    // (the waterfall shows 10 rows, unlike the 100-point sparklines).
                    slicer_history.push_back(SlicerInterval { timestamp: the_time, counts: slicer_interval.clone() });
                    while slicer_history.len() > 10 {
                        slicer_history.pop_front();
                    }

                    let slicer_data = SlicerTelemetry {
                        name: String::from("slicer_statistics"),
                        timestamp: the_time,
                        microsecs,
                        slicer_count,
                        slicer_gains: slicer_gains.clone(),
                        intervals: slicer_history.clone(),
                        lifetime_slicer_hits: lifetime_slicer_hits.clone(),
                    };

                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::SlicerStatus(slicer_data))) {
                        warn!("Failed to send slicer statistics to channel: {}", e);
                    }

                    // Evict stations not heard in the last 36 hours, capturing the
                    // evicted keys so their rows can be removed from the store.
                    let evict_threshold = chrono::Duration::hours(36);
                    let now = Local::now();
                    let mut evicted_stations: Vec<String> = Vec::new();
                    station_map.retain(|k, entry| {
                        let keep = now - entry.last_heard < evict_threshold;
                        if !keep {
                            evicted_stations.push(k.clone());
                        }
                        keep
                    });

                    // Prune the satellite packet log of entries older than 24 hours,
                    // capturing the removed keys for the store.
                    let sat_log_threshold = chrono::Duration::hours(24);
                    let mut pruned_sat_keys: Vec<i64> = Vec::new();
                    if let Ok(mut log) = sat_packet_log.write() {
                        while let Some(front) = log.front() {
                            if now - front.receivetime > sat_log_threshold {
                                if let Some(p) = log.pop_front() {
                                    pruned_sat_keys.push(sat_packet_key(&p));
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    // Prune frequencies not heard in the last 24 hours, capturing the
                    // evicted keys for the store.
                    let freq_threshold = chrono::Duration::hours(24);
                    let mut evicted_freqs: Vec<String> = Vec::new();
                    freq_counts.retain(|k, (_, last_heard)| {
                        let keep = now - *last_heard < freq_threshold;
                        if !keep {
                            evicted_freqs.push(k.clone());
                        }
                        keep
                    });

                    // Emit station statistics
                    let mut stations: Vec<StationEntry> = station_map.values().cloned().collect();
                    stations.sort_by_key(|s| std::cmp::Reverse(s.count));

                    let mut frequencies: Vec<FrequencyCount> = freq_counts.iter()
                        .map(|(f, (c, _))| FrequencyCount { frequency: f.clone(), count: *c })
                        .collect();
                    frequencies.sort_by_key(|f| std::cmp::Reverse(f.count));

                    let station_data = StationTelemetry {
                        name: String::from("station_statistics"),
                        stations,
                        frequencies,
                    };

                    if let Err(e) = data_channel.send(DataItem::Tlm(AppTelemetry::StationStatus(station_data))) {
                        warn!("Failed to send station statistics to channel: {}", e);
                    }

                    // reset per-interval counters
                    heard_direct = 0;
                    digipeated = 0;
                    decode_errors = 0;
                    total_packets = 0;
                    for c in slicer_interval.iter_mut() {
                        *c = 0;
                    }

                    //#################
                    // Persist this interval's state to the store. Failures are
                    // logged but non-fatal — the in-memory state stays authoritative
                    // and the next tick retries.
                    //#################
                    persist_packet_lifetime(
                        &store,
                        lifetime_total_packets,
                        lifetime_heard_direct,
                        lifetime_digipeated,
                        lifetime_decode_errors,
                    );
                    if slicer_count > 0
                        && let Err(e) = store.save_slicer(
                            slicer_count,
                            &slicer_gains,
                            &lifetime_slicer_hits,
                            slicer_history.make_contiguous(),
                        )
                    {
                        warn!("Failed to persist slicer state: {}", e);
                    }
                    // Remove evicted rows first, then upsert the still-present dirty
                    // rows (an evicted key won't also be dirty this interval).
                    for k in &evicted_stations {
                        dirty_stations.remove(k);
                        if let Err(e) = store.delete_station(k) {
                            warn!("Failed to delete evicted station {}: {}", k, e);
                        }
                    }
                    for k in &evicted_freqs {
                        dirty_freqs.remove(k);
                        if let Err(e) = store.delete_freq(k) {
                            warn!("Failed to delete evicted frequency {}: {}", k, e);
                        }
                    }
                    for k in dirty_stations.drain() {
                        if let Some(entry) = station_map.get(&k)
                            && let Err(e) = store.upsert_station(entry)
                        {
                            warn!("Failed to persist station {}: {}", k, e);
                        }
                    }
                    for k in dirty_freqs.drain() {
                        if let Some((count, last_heard)) = freq_counts.get(&k)
                            && let Err(e) = store.upsert_freq(&k, *count, last_heard)
                        {
                            warn!("Failed to persist frequency {}: {}", k, e);
                        }
                    }
                    if let Err(e) = store.delete_sat_packets(&pruned_sat_keys) {
                        warn!("Failed to delete pruned satellite packets: {}", e);
                    }

                    // Prune the rolling packet-history window to its configured
                    // length (default 6h) so the all-packets log stays bounded.
                    let packet_cutoff = now - chrono::Duration::hours(config.packet_history_hours() as i64);
                    if let Err(e) = store.prune_packets(packet_cutoff.timestamp_micros()) {
                        warn!("Failed to prune packet history: {}", e);
                    }
                },


                // read the next decoded frame from the multicast stream
                result = sub.recv_frame() => {
                    match result {

                        // a frame arrived
                        Ok((frame, _from)) => {

                            // Learn the slicer bank size and gain ladder from the
                            // stream on the first frame that carries it. Static for
                            // the producer session, so this runs once.
                            if slicer_count == 0
                                && let Some(g) =
                                    frame.rf.slicer_gains.as_ref().filter(|g| !g.is_empty())
                            {
                                slicer_count = g.len();
                                slicer_gains = g.clone();
                                slicer_zones = g.iter().map(|x| slicer_zone(*x)).collect();
                                slicer_db = g.iter().map(|x| 20.0 * x.log10()).collect();
                                slicer_interval = vec![0; slicer_count];

                                // Apply the restored slicer accumulators only when
                                // they match the live bank size. A different length
                                // means the producer's slicer bank changed while we
                                // were down, so the slicer identities no longer line
                                // up — start fresh in that case.
                                match restored_slicer.take() {
                                    Some(r) if r.lifetime_hits.len() == slicer_count => {
                                        lifetime_slicer_hits = r.lifetime_hits;
                                        slicer_history = r.history.into();
                                        info!("Restored slicer statistics ({} slicers)", slicer_count);
                                    }
                                    Some(_) => {
                                        warn!(
                                            "Slicer bank size changed since last run; resetting slicer statistics"
                                        );
                                        lifetime_slicer_hits = vec![0; slicer_count];
                                    }
                                    None => {
                                        lifetime_slicer_hits = vec![0; slicer_count];
                                    }
                                }
                            }

                            // map the stream frame into our internal RTPPacket. A
                            // frame without ax25_meta (a pre-v2 producer) can't be
                            // mapped faithfully — skip it rather than guess.
                            let mapped = match map_frame(&frame) {
                                Some(m) => m,
                                None => {
                                    debug!("frame without ax25_meta; skipping");
                                    continue;
                                }
                            };
                            let MappedPacket { mut packet, symbol_table: sym_table, symbol_code: sym_code } = mapped;

                            // Apply runtime-config-driven flags. is_satellite must be
                            // set before droppacket() so the sat-frequency policy fires.
                            packet.is_satellite = sat_freqs.contains(&packet.frequency);
                            packet.igated = crate::igate::droppacket(&packet).is_none();

                            // Summarize which slicers decoded this frame into the
                            // per-packet twist bar payload. Skipped when no slicer
                            // fired so the frontend shows a neutral placeholder.
                            if packet.slicer_mask != 0 && slicer_count > 0 {
                                // mean twist (dB) over the slicers that fired
                                let (mut sum, mut n) = (0.0f32, 0u32);
                                for (i, db) in slicer_db.iter().enumerate() {
                                    if packet.slicer_mask & (1 << i) != 0 {
                                        sum += *db;
                                        n += 1;
                                    }
                                }
                                packet.twist = Some(TwistInfo {
                                    cols: slicer_count,
                                    mask: packet.slicer_mask,
                                    zones: slicer_zones.clone(),
                                    centroid_db: if n > 0 { sum / n as f32 } else { 0.0 },
                                });
                            }
                            let p = packet;

                            if p.heard_direct {
                                heard_direct += 1;
                                lifetime_heard_direct += 1;
                            } else {
                                digipeated += 1;
                                lifetime_digipeated += 1;
                            }
                            total_packets += 1;
                            lifetime_total_packets += 1;

                            // tally each slicer that demodulated this frame for the
                            // diversity waterfall (a frame may be decoded by several).
                            for s in 0..slicer_count {
                                if p.slicer_mask & (1 << s) != 0 {
                                    slicer_interval[s] += 1;
                                    lifetime_slicer_hits[s] += 1;
                                }
                            }

                            // update station tracking
                            let freq_key = format!("{:.3}", p.frequency);
                            dirty_freqs.insert(freq_key.clone());
                            let freq_entry = freq_counts.entry(freq_key).or_insert((0, p.receivetime));
                            freq_entry.0 += 1;
                            freq_entry.1 = p.receivetime;

                            // use object/item name as station key if present
                            let station_key = p.object_name.clone().unwrap_or_else(|| p.source.clone());
                            dirty_stations.insert(station_key.clone());
                            let transmitted_by = p.object_name.as_ref().map(|_| p.source.clone());

                            let entry = station_map.entry(station_key.clone()).or_insert_with(|| StationEntry {
                                callsign: station_key,
                                transmitted_by: transmitted_by.clone(),
                                last_heard: p.receivetime,
                                frequency: p.frequency,
                                latitude: None,
                                longitude: None,
                                altitude_ft: None,
                                heard_direct: p.heard_direct,
                                position_path: p.digipeater_path.clone(),
                                position_hops: p.hops,
                                altitude_path: p.digipeater_path.clone(),
                                altitude_hops: p.hops,
                                symbol_table: None,
                                symbol_code: None,
                                count: 0,
                                count_direct: 0,
                                count_digipeated: 0,
                            });
                            entry.last_heard = p.receivetime;
                            entry.frequency = p.frequency;
                            entry.heard_direct = p.heard_direct;
                            if let Some(ref tb) = transmitted_by {
                                entry.transmitted_by = Some(tb.clone());
                            }
                            entry.count += 1;
                            if p.heard_direct {
                                entry.count_direct += 1;
                            } else {
                                entry.count_digipeated += 1;
                            }
                            if p.latitude.is_some() && p.longitude.is_some() {
                                entry.latitude = p.latitude;
                                entry.longitude = p.longitude;
                                entry.position_path = p.digipeater_path.clone();
                                entry.position_hops = p.hops;
                            }
                            if let Some(alt) = p.altitude_ft {
                                if entry.altitude_ft.is_none_or(|prev| alt > prev) {
                                    entry.altitude_path = p.digipeater_path.clone();
                                    entry.altitude_hops = p.hops;
                                }
                                entry.altitude_ft = Some(entry.altitude_ft.map_or(alt, |prev| prev.max(alt)));
                            }
                            if let Some(st) = sym_table {
                                entry.symbol_table = Some(st);
                            }
                            if let Some(sc) = sym_code {
                                entry.symbol_code = Some(sc);
                            }

                            // log this
                            debug!("{:3.3}MHz Direct: {}  {}", p.frequency, p.heard_direct as u32, p.raw);

                            // append to the 24h satellite packet log (newest-first
                            // ordering is maintained at read time) and persist it so
                            // the log survives a restart.
                            if p.is_satellite {
                                if let Ok(mut log) = sat_packet_log.write() {
                                    log.push_back(p.clone());
                                }
                                if let Err(e) = store.insert_sat_packet(&p) {
                                    warn!("Failed to persist satellite packet: {}", e);
                                }
                            }

                            // Persist every heard packet into the rolling
                            // packet-history window ([storage] packet_history) so any
                            // station can be monitored with full backfill. Pruned on
                            // the periodic tick below.
                            if let Err(e) = store.insert_packet(&p) {
                                warn!("Failed to persist packet: {}", e);
                            }

                            // attempt to send this packet to the channel so downstream
                            // consumers can process this packet.
                            if let Err(e) = data_channel.send(DataItem::Pkt(Packet::RTP(p))) {
                                warn!("Channel send failed: {}", e);
                            }
                        },

                        // a malformed / version-incompatible datagram: skip and keep going.
                        Err(RecvError::Codec(e)) => {
                            debug!("Skipping bad datagram: {}", e);
                        },

                        // a socket-level error: tear down and rebuild the subscriber.
                        Err(RecvError::Io(e)) => {
                            error!("APRS stream receive failed: {}. Will reconnect...", e);
                            break;
                        },
                    }
                },
            }
        } // inner loop
    } // outer reconnection loop

    //#################
    // Final flush on shutdown: persist the lifetime counters, slicer state, and any
    // stations/frequencies touched since the last tick so a graceful stop loses
    // nothing. Evictions were already mirrored on the last tick.
    //#################
    persist_packet_lifetime(
        &store,
        lifetime_total_packets,
        lifetime_heard_direct,
        lifetime_digipeated,
        lifetime_decode_errors,
    );
    if slicer_count > 0
        && let Err(e) = store.save_slicer(
            slicer_count,
            &slicer_gains,
            &lifetime_slicer_hits,
            slicer_history.make_contiguous(),
        )
    {
        warn!("Failed to persist slicer state on shutdown: {}", e);
    }
    for k in dirty_stations.drain() {
        if let Some(entry) = station_map.get(&k)
            && let Err(e) = store.upsert_station(entry)
        {
            warn!("Failed to persist station {} on shutdown: {}", k, e);
        }
    }
    for k in dirty_freqs.drain() {
        if let Some((count, last_heard)) = freq_counts.get(&k)
            && let Err(e) = store.upsert_freq(&k, *count, last_heard)
        {
            warn!("Failed to persist frequency {} on shutdown: {}", k, e);
        }
    }

    // drop the channel
    drop(data_channel);

    info!("Task ended.");

    Ok(())
}

// Persist the rtp_listener lifetime counters to the store, logging (but not
// propagating) any failure so a transient DB error never kills the listener.
fn persist_packet_lifetime(
    store: &Store,
    total: u64,
    heard_direct: u64,
    digipeated: u64,
    decode_errors: u64,
) {
    let rec = crate::store::PacketLifetime {
        id: 0,
        total,
        heard_direct,
        digipeated,
        decode_errors,
    };
    if let Err(e) = store.save_packet_lifetime(&rec) {
        warn!("Failed to persist packet lifetime counters: {}", e);
    }
}

// A mapped RTPPacket together with the APRS symbol table/code parsed from the
// same packet (used to populate per-station symbol info).
struct MappedPacket {
    packet: RTPPacket,
    symbol_table: Option<char>,
    symbol_code: Option<char>,
}

// Map a decoded stream frame into the internal RTPPacket type. The AX.25 framing
// facts (source/destination/path/heard-direct/dti) come straight from the frame's
// `ax25_meta` block — decoded once upstream by the producer — so nothing is
// re-parsed here. The APRS payload (position/object/item/symbol) is read from the
// frame's already-parsed packet, falling back to decoding the TNC2 text only if
// the producer couldn't type it.
//
// Returns None if the frame carries no `ax25_meta` (a pre-v2 producer): without
// it we can't recover source/dest/heard faithfully, so the caller skips the frame.
fn map_frame(frame: &AprsFrame) -> Option<MappedPacket> {
    let meta = frame.ax25_meta.as_ref()?;

    // wall-clock receive time reconstructed from the frame's epoch-millis stamp;
    // pair it with a fresh monotonic Instant for the staleness guard (channel
    // delivery is effectively immediate).
    let receivetime = DateTime::<Utc>::from_timestamp_millis(frame.capture.received_at_ms as i64)
        .map(|dt| dt.with_timezone(&Local))
        .unwrap_or_else(Local::now);
    let received_instant = Instant::now();

    // The verbatim 8-bit info field, sliced out of the raw AX.25 using the
    // producer's offset — no AX.25 re-parsing. `info` is the lossy-UTF-8 render
    // for display/dedup/SSE; `info_bytes` is the faithful payload for igating.
    let info_bytes: Vec<u8> = meta
        .info_offset
        .and_then(|off| frame.ax25.get(off as usize..))
        .map(|s| s.to_vec())
        .unwrap_or_default();
    let info = String::from_utf8_lossy(&info_bytes).to_string();
    let info_invalid_bytes = meta.info_invalid_bytes as usize;

    // viapath and filtered digipeater path (real callsigns only). The path is the
    // via callsigns joined without heard (`*`) markers, matching the form the
    // igate reformer expects.
    let path = meta
        .via
        .iter()
        .map(|h| h.call.clone())
        .collect::<Vec<_>>()
        .join(",");
    let digipeater_path: Vec<String> = meta
        .via
        .iter()
        .map(|h| &h.call)
        .filter(|s| EXCLUDED_ADDRS.iter().all(|x| !s.contains(x)))
        .cloned()
        .collect();
    let hops = digipeater_path.len() as u32;

    // APRS data type identifier (first info byte)
    let ptype: char = meta
        .dti
        .map(|b| b as char)
        .or_else(|| info.chars().next())
        .unwrap_or('\0');

    // strict "any digipeater touched this" flag (includes WIDE-class fill-ins)
    let was_digipeated = meta.via.iter().any(|h| h.heard);

    // RF-only packets must not be igated
    let rfonly = meta
        .via
        .iter()
        .any(|h| RFONLY_ADDRS.iter().any(|x| h.call.contains(x)));

    // frequency in MHz from the RF metadata (or the SSRC, ka9q's kHz convention)
    let frequency = frame
        .rf
        .frequency_hz
        .map(|hz| hz as f64 / 1_000_000.0)
        .or_else(|| frame.capture.ssrc.map(|s| s as f64 / 1000.0))
        .unwrap_or(0.0);

    // Canonical TNC2 text for display, reconstructed from the parsed packet (this
    // includes the heard `*` markers). Falls back to a hand-built header when the
    // frame couldn't be typed or re-encoded.
    let raw = frame
        .parsed
        .as_ref()
        .and_then(|p| p.encode_textual().ok())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_else(|| {
            if path.is_empty() {
                format!("{}>{}:{}", meta.source, meta.destination, info)
            } else {
                format!("{}>{},{}:{}", meta.source, meta.destination, path, info)
            }
        });

    // parse position/object/item data and the map symbol. Prefer the payload the
    // producer already parsed; only decode the TNC2 text if it wasn't typed.
    let (mut latitude, mut longitude, mut altitude_ft) = (None, None, None);
    let (mut speed_mph, mut course_deg) = (None, None);
    let mut object_name: Option<String> = None;
    let (mut symbol_table, mut symbol_code) = (None, None);

    let parsed = frame
        .parsed
        .clone()
        .or_else(|| DecodedPacket::decode_textual(raw.as_bytes()).ok());
    if let Some(parsed) = parsed {
        match parsed.data {
            AprsData::Position(pos) => {
                latitude = Some(pos.position.latitude.value());
                longitude = Some(pos.position.longitude.value());
                altitude_ft = pos.position.altitude.map(|a| a.feet);
                (speed_mph, course_deg) = course_speed_from_extension(pos.extension.as_ref());
                symbol_table = Some(pos.position.symbol.table);
                symbol_code = Some(pos.position.symbol.code);
            }
            AprsData::MicE(mice) => {
                latitude = Some(mice.latitude.value());
                longitude = Some(mice.longitude.value());
                // aprs-decode reports Mic-E altitude in metres; convert to feet.
                altitude_ft = mice.altitude_m.map(|m| m * 3.28084);
                // Mic-E always carries course/speed; course 000 means unknown.
                speed_mph = Some(mice.speed.knots() as f64 * KNOTS_TO_MPH);
                let course = mice.course.degrees();
                course_deg = if course == 0 {
                    None
                } else {
                    Some(course as u16)
                };
                symbol_table = Some(mice.symbol_table);
                symbol_code = Some(mice.symbol_code);
            }
            AprsData::Object(obj) => {
                object_name = Some(String::from_utf8_lossy(&obj.name).to_string());
                latitude = Some(obj.position.latitude.value());
                longitude = Some(obj.position.longitude.value());
                altitude_ft = obj.position.altitude.map(|a| a.feet);
                (speed_mph, course_deg) = course_speed_from_extension(obj.extension.as_ref());
                symbol_table = Some(obj.position.symbol.table);
                symbol_code = Some(obj.position.symbol.code);
            }
            AprsData::Item(item) => {
                object_name = Some(String::from_utf8_lossy(&item.name).to_string());
                latitude = Some(item.position.latitude.value());
                longitude = Some(item.position.longitude.value());
                altitude_ft = item.position.altitude.map(|a| a.feet);
                (speed_mph, course_deg) = course_speed_from_extension(item.extension.as_ref());
                symbol_table = Some(item.position.symbol.table);
                symbol_code = Some(item.position.symbol.code);
            }
            _ => {}
        }
    }

    // is_satellite and igated are set by rtp_listener after mapping, since they
    // depend on runtime config.
    let packet = RTPPacket {
        receivetime,
        received_instant,
        raw,
        info,
        info_bytes,
        path,
        digipeater_path,
        hops,
        ptype,
        source: meta.source.clone(),
        destination: meta.destination.clone(),
        heard_direct: meta.heard_direct,
        heardfrom: meta.heard_from.clone(),
        was_digipeated,
        rfonly,
        frequency,
        is_satellite: false,
        igated: false,
        info_invalid_bytes,
        object_name,
        latitude,
        longitude,
        altitude_ft,
        speed_mph,
        course_deg,
        slicer_mask: frame.rf.slicer_mask.unwrap_or(0),
        twist: None,
    };

    Some(MappedPacket {
        packet,
        symbol_table,
        symbol_code,
    })
}
