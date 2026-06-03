use tokio::{sync::broadcast, time::{interval, sleep, Duration}};
use tokio_util::sync::CancellationToken;
use std::{collections::{HashMap, VecDeque}, sync::Arc, fmt, time::Instant};
use serde::Serialize;
use chrono::{DateTime, Local, Utc};

use log::{info, warn, error, debug};

use aprs_rtp::{AprsListener, config::{SourceConfig, DecoderConfig}};
use aprs_decode::packet::{AprsPacket as DecodedPacket, AprsData};

use crate::config::{Config, AppTelemetry, PacketTelemetry, SlicerTelemetry, SlicerInterval, DataSeries, DataPoint, DataItem, StationEntry, StationTelemetry, FrequencyCount};
use crate::error::RtpigateError;

// Per-slicer space-gain ladder, replicating aprs-rtp's `afsk::slicer::space_gains`
// (which is crate-private). Each slicer applies `demod_out = mark - space * gain`,
// so `gain < 1` favors loud-space (pre-emphasized) signals and `gain > 1` favors
// loud-mark (de-emphasized) signals. The ladder is a geometric progression from
// `min_g` to `max_g` over `n` rungs; a single slicer uses unity gain. Kept in sync
// with the DecoderConfig we pass to the listener so the frontend labels stay truthful.
fn space_gains(n: usize, min_g: f32, max_g: f32) -> Vec<f32> {
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![1.0];
    }
    let step = (max_g / min_g).powf(1.0 / (n - 1) as f32);
    let mut gains = vec![min_g];
    for i in 1..n {
        gains.push(gains[i - 1] * step);
    }
    gains
}

// the packet structure (created by the RTP thread for incoming RTP packets)
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

    // the frequency from the RTP packet usually from ssrc()
    pub frequency: f64,

    // was this packet heard from or perhaps, destined to a satellite?
    pub is_satellite: bool,

    // whether this packet would be igated by droppacket() at receive time.
    // Mirrors what aprs_is.rs will decide, minus the dedup step — duplicates
    // within the gating window are still counted as "would-igate" here.
    pub igated: bool,

    // object or item name (if this packet is an object/item report)
    pub object_name: Option<String>,

    // parsed position data (if available)
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude_ft: Option<f64>,

    // bitmask of demodulator slicers that decoded this frame (bit i = slicer i).
    // Used only for the slicer-waterfall aggregation; not serialised per-packet.
    #[serde(skip)]
    pub slicer_mask: u16,
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
            format!("{}>{},qAO,{}:{}", self.source, self.destination, callsign, self.info)
        } else {
            format!("{}>{},{},qAO,{}:{}", self.source, self.destination, self.path, callsign, self.info)
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
            format!("{}>{},{},qAO,{}:", self.source, self.destination, self.path, callsign)
        };
        let mut out = header.into_bytes();
        out.extend_from_slice(&self.info_bytes);
        out
    }
}

// type of packets
#[derive(Debug, Clone)]
pub enum Packet {
    RTP(RTPPacket),
}


// Constant slices for packet classification — no heap allocation per packet
const EXCLUDED_ADDRS: &[&str] = &["WIDE", "TCPIP", "NOGATE", "RFONLY", "SGATE"];
const RFONLY_ADDRS: &[&str] = &["TCPIP", "TCPXX", "RFONLY", "NOGATE"];


// Listens to a ka9q-radio RTP multicast audio group via the `aprs-rtp` crate,
// which performs RTP dejitter, 1200-baud AFSK demodulation, HDLC framing, CRC
// validation and AX.25 parsing internally. Decoded packets are mapped into the
// internal RTPPacket type and broadcast on the shared data channel.
//
// Normally this never returns — it loops forever, reconnecting on failure.
pub async fn rtp_listener(
    data_channel: broadcast::Sender<DataItem>,
    token: CancellationToken,
    config: Arc<Config>,
    sat_packet_log: Arc<std::sync::RwLock<VecDeque<RTPPacket>>>,
) -> Result<(), RtpigateError> {

    info!("Started");

    // satellite frequencies sourced from config (default [145.825] if unset)
    let sat_freqs = config.satellite_frequencies();

    // per-interval statistics
    let mut heard_direct = 0;
    let mut digipeated = 0;
    // `decode_errors` is retained for telemetry/frontend compatibility. The
    // aprs-rtp crate only emits successfully-decoded packets over the channel,
    // so this counter is never incremented and always reports 0.
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

    // decoder configuration; `slicers` (default 8) drives the waterfall column
    // count and is the single source of truth for the slicer bank size.
    let decoder = DecoderConfig::default();
    let slicer_count = decoder.slicers;
    let slicer_gains = space_gains(slicer_count, decoder.min_gain, decoder.max_gain);

    // per-slicer accumulators for the slicer-diversity waterfall. `slicer_interval`
    // counts demodulations in the current 15s window; `slicer_history` keeps the
    // last 10 windows (heatmap rows); `lifetime_slicer_hits` never resets.
    let mut slicer_interval: Vec<u32> = vec![0; slicer_count];
    let mut slicer_history: VecDeque<SlicerInterval> = VecDeque::new();
    let mut lifetime_slicer_hits: Vec<u64> = vec![0; slicer_count];

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

        // build the aprs-rtp source/decoder configuration. The [rtp] section
        // now points at a ka9q-radio channel *audio* multicast group; the
        // decoder demodulates the PCM stream itself. Tuning knobs use crate
        // defaults (8 slicers, single-bit CRC fix, 2-packet jitter buffer).
        let source = SourceConfig {
            host: config.rtp.host.clone(),
            port: config.rtp.port as u16,
            interface: None,
            jitter_buffer: 2,
            ssrc: Vec::new(),
        };

        // start the listener; on failure back off and retry just like the
        // previous socket-setup path did.
        let mut rx = match AprsListener::new(source, decoder.clone()).run().await {
            Ok(rx) => {
                backoff_secs = 5;
                rx
            },
            Err(e) => {
                error!("APRS RTP listener setup failed: {}. Retrying in {}s...", e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        info!("Connected to RTP multicast audio: {}:{}", config.rtp.host, config.rtp.port);

        // inner packet read loop
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

                    // Evict stations not heard in the last 36 hours
                    let evict_threshold = chrono::Duration::hours(36);
                    let now = Local::now();
                    station_map.retain(|_, entry| now - entry.last_heard < evict_threshold);

                    // Prune the satellite packet log of entries older than 24 hours.
                    let sat_log_threshold = chrono::Duration::hours(24);
                    if let Ok(mut log) = sat_packet_log.write() {
                        while let Some(front) = log.front() {
                            if now - front.receivetime > sat_log_threshold {
                                log.pop_front();
                            } else {
                                break;
                            }
                        }
                    }
                    // Prune frequencies not heard in the last 24 hours
                    let freq_threshold = chrono::Duration::hours(24);
                    freq_counts.retain(|_, (_, last_heard)| now - *last_heard < freq_threshold);

                    // Emit station statistics
                    let mut stations: Vec<StationEntry> = station_map.values().cloned().collect();
                    stations.sort_by(|a, b| b.count.cmp(&a.count));

                    let mut frequencies: Vec<FrequencyCount> = freq_counts.iter()
                        .map(|(f, (c, _))| FrequencyCount { frequency: f.clone(), count: *c })
                        .collect();
                    frequencies.sort_by(|a, b| b.count.cmp(&a.count));

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

                },


                // read the next decoded packet from the aprs-rtp channel
                maybe_pkt = rx.recv() => {
                    match maybe_pkt {

                        // a packet was decoded
                        Some(rtp_pkt) => {

                            // map the aprs-rtp packet into our internal RTPPacket,
                            // running aprs-decode for position/object/item/symbol data.
                            let MappedPacket { mut packet, symbol_table: sym_table, symbol_code: sym_code } = map_packet(rtp_pkt);

                            // Apply runtime-config-driven flags. is_satellite must be
                            // set before droppacket() so the sat-frequency policy fires.
                            packet.is_satellite = sat_freqs.contains(&packet.frequency);
                            packet.igated = crate::igate::droppacket(&packet).is_none();
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
                            let freq_entry = freq_counts.entry(freq_key).or_insert((0, p.receivetime));
                            freq_entry.0 += 1;
                            freq_entry.1 = p.receivetime;

                            // use object/item name as station key if present
                            let station_key = p.object_name.clone().unwrap_or_else(|| p.source.clone());
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
                            // ordering is maintained at read time).
                            if p.is_satellite {
                                if let Ok(mut log) = sat_packet_log.write() {
                                    log.push_back(p.clone());
                                }
                            }

                            // attempt to send this packet to the channel so downstream
                            // consumers can process this packet.
                            if let Err(e) = data_channel.send(DataItem::Pkt(Packet::RTP(p))) {
                                warn!("Channel send failed: {}", e);
                            }
                        },

                        // the aprs-rtp listener closed its channel - break inner loop to reconnect
                        None => {
                            error!("APRS RTP listener channel closed. Will reconnect...");
                            break;
                        },
                    }
                },
            }
        } // inner loop
    } // outer reconnection loop

    // drop the channel
    drop(data_channel);

    info!("Task ended.");

    Ok(())
}


// A mapped RTPPacket together with the APRS symbol table/code parsed from the
// same packet (used to populate per-station symbol info).
struct MappedPacket {
    packet: RTPPacket,
    symbol_table: Option<char>,
    symbol_code: Option<char>,
}

// Map an aprs-rtp decoded packet into the internal RTPPacket type. The AX.25
// framing (source/destination/path/heard-direct) is taken directly from the
// aprs-rtp packet, and the APRS payload is re-parsed with aprs-decode to
// extract position, altitude, object/item names and the map symbol.
fn map_packet(pkt: aprs_rtp::AprsPacket) -> MappedPacket {

    // wall-clock receive time from the decoder; pair it with a fresh monotonic
    // Instant for the staleness guard (channel delivery is effectively immediate).
    let receivetime = DateTime::<Local>::from(pkt.received_at);
    let received_instant = Instant::now();

    // the information field as UTF-8 text (lossy for binary Mic-E/telemetry),
    // plus the original bytes kept verbatim for byte-faithful igating.
    let info = String::from_utf8_lossy(&pkt.info).to_string();
    let info_bytes = pkt.info.clone();

    // viapath and filtered digipeater path (real callsigns only)
    let path = pkt.via.join(",");
    let digipeater_path: Vec<String> = pkt.via.iter()
        .filter(|s| EXCLUDED_ADDRS.iter().all(|x| !s.contains(x)))
        .cloned()
        .collect();
    let hops = digipeater_path.len() as u32;

    // APRS data type identifier (first info byte)
    let ptype: char = pkt.dti
        .map(|b| b as char)
        .or_else(|| info.chars().next())
        .unwrap_or('\0');

    // strict "any digipeater touched this" flag (includes WIDE-class fill-ins)
    let was_digipeated = pkt.via_heard.iter().any(|&h| h);

    // RF-only packets must not be igated
    let rfonly = pkt.via.iter().any(|s| RFONLY_ADDRS.iter().any(|x| s.contains(x)));

    // parse position/object/item data and the map symbol using aprs-decode.
    // Prefer the validated AX.25 bytes; fall back to the TNC2 text on error.
    let (mut latitude, mut longitude, mut altitude_ft) = (None, None, None);
    let mut object_name: Option<String> = None;
    let (mut symbol_table, mut symbol_code) = (None, None);

    let decoded = DecodedPacket::decode_ax25(&pkt.raw_ax25)
        .or_else(|_| DecodedPacket::decode_textual(pkt.text.as_bytes()));
    if let Ok(parsed) = decoded {
        match parsed.data {
            AprsData::Position(pos) => {
                latitude = Some(pos.position.latitude.value());
                longitude = Some(pos.position.longitude.value());
                altitude_ft = pos.position.altitude.map(|a| a.feet);
                symbol_table = Some(pos.position.symbol.table);
                symbol_code = Some(pos.position.symbol.code);
            },
            AprsData::MicE(mice) => {
                latitude = Some(mice.latitude.value());
                longitude = Some(mice.longitude.value());
                // aprs-decode reports Mic-E altitude in metres; convert to feet.
                altitude_ft = mice.altitude_m.map(|m| m * 3.28084);
                symbol_table = Some(mice.symbol_table);
                symbol_code = Some(mice.symbol_code);
            },
            AprsData::Object(obj) => {
                object_name = Some(String::from_utf8_lossy(&obj.name).to_string());
                latitude = Some(obj.position.latitude.value());
                longitude = Some(obj.position.longitude.value());
                altitude_ft = obj.position.altitude.map(|a| a.feet);
                symbol_table = Some(obj.position.symbol.table);
                symbol_code = Some(obj.position.symbol.code);
            },
            AprsData::Item(item) => {
                object_name = Some(String::from_utf8_lossy(&item.name).to_string());
                latitude = Some(item.position.latitude.value());
                longitude = Some(item.position.longitude.value());
                altitude_ft = item.position.altitude.map(|a| a.feet);
                symbol_table = Some(item.position.symbol.table);
                symbol_code = Some(item.position.symbol.code);
            },
            _ => {},
        }
    }

    // is_satellite and igated are set by rtp_listener after mapping, since they
    // depend on runtime config.
    let packet = RTPPacket {
        receivetime,
        received_instant,
        raw: pkt.text,
        info,
        info_bytes,
        path,
        digipeater_path,
        hops,
        ptype,
        source: pkt.source,
        destination: pkt.destination,
        heard_direct: pkt.heard_direct,
        heardfrom: pkt.heard_from,
        was_digipeated,
        rfonly,
        frequency: pkt.freq_mhz,
        is_satellite: false,
        igated: false,
        object_name,
        latitude,
        longitude,
        altitude_ft,
        slicer_mask: pkt.slicer_mask,
    };

    MappedPacket { packet, symbol_table, symbol_code }
}
