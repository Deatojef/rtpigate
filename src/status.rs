// status.rs — subscribe to radiod's TLV status multicast and maintain a shared
// HashMap<SSRC, FM_SNR_dB> consumed by the RTP listener.
//
// Wire format reference: ka9q-radio/src/status.c and ka9q-radio/src/status.h.
// Each datagram begins with a 1-byte packet type (STATUS=0, CMD=1) followed by
// a sequence of TLV tuples and terminated by an EOL (type=0) tag. Per tuple:
//   * 1 byte  type tag (enum status_type)
//   * 1 byte  length, OR if the high bit is set, the low 7 bits give the number
//             of big-endian length bytes that follow.
//   * N bytes value. Integers are big-endian with leading zeros suppressed,
//             so an int32 with value 0 is encoded as length=0 (and floats are
//             encoded by reinterpreting their bits as a uint32 and going through
//             the same int32 path — meaning a 0.0 float also lands as length=0).

use std::{collections::{HashMap, VecDeque}, sync::Arc, time::{Duration, Instant}};

use tokio::{sync::RwLock, time::sleep};
use tokio_util::sync::CancellationToken;
use log::{info, warn, error};

use crate::config::StatusConfig;
use crate::error::RtpigateError;
use crate::ka9q::{setup_multicast_socket, SnrCluster, SnrMap};

/// Maximum gap between consecutive status samples that still counts as the
/// same carrier event. The observed inter-sample spacing within an active
/// burst is typically 150–360 ms; pick something comfortably above that.
pub const CLUSTER_GAP: Duration = Duration::from_millis(500);

/// How long to keep unmatched clusters around before discarding. Generous
/// enough to absorb packetd's worst-case decode/buffer latency.
const CLUSTER_RETENTION: Duration = Duration::from_secs(30);

// Status-packet type byte
const STATUS_TYPE: u8 = 0;

// Tag IDs we care about (positions within `enum status_type` in
// ka9q-radio/src/status.h). Counted from EOL=0.
const TAG_EOL: u8 = 0;
const TAG_OUTPUT_SSRC: u8 = 18;
const TAG_FM_SNR: u8 = 66;

pub fn new_snr_map() -> SnrMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub async fn status_listener(
    config: StatusConfig,
    snr_map: SnrMap,
    token: CancellationToken,
) -> Result<(), RtpigateError> {
    info!("Started");

    let address = format!("{}:{}", config.host, config.port);

    let mut backoff_secs: u64 = 5;
    const MAX_BACKOFF_SECS: u64 = 300;

    let mut buf = [0u8; 8192];

    loop {
        if token.is_cancelled() {
            break;
        }

        let udp_socket = match setup_multicast_socket(&address) {
            Ok(s) => {
                backoff_secs = 5;
                s
            },
            Err(e) => {
                error!("Status socket setup failed: {}. Retrying in {}s...", e, backoff_secs);
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = sleep(Duration::from_secs(backoff_secs)) => {},
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            },
        };

        info!("Connected to status multicast: {}", address);

        loop {
            tokio::select! {
                _ = token.cancelled() => break,

                result = udp_socket.recv(&mut buf) => {
                    match result {
                        Ok(num_bytes) => {
                            if let Some((ssrc, snr_db)) = parse_status_tlv(&buf[..num_bytes]) {
                                let now = Instant::now();
                                let mut map = snr_map.write().await;
                                let deque = map.entry(ssrc).or_insert_with(VecDeque::new);

                                // FIFO: push_back for new arrivals, pop_front for oldest.
                                // Extend the back (current open cluster) if this sample is
                                // within CLUSTER_GAP of the previous; otherwise start a new
                                // cluster at the back.
                                let extend = deque.back()
                                    .map(|c| now.duration_since(c.last_sample_at) < CLUSTER_GAP)
                                    .unwrap_or(false);
                                if extend {
                                    if let Some(c) = deque.back_mut() {
                                        c.extend(snr_db, now);
                                    }
                                } else {
                                    deque.push_back(SnrCluster::new(snr_db, now));
                                }

                                // Prune stale clusters from the front.
                                let cutoff = now.checked_sub(CLUSTER_RETENTION).unwrap_or(now);
                                while let Some(front) = deque.front() {
                                    if front.last_sample_at < cutoff {
                                        deque.pop_front();
                                    } else {
                                        break;
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            warn!("Status socket recv error: {}. Reconnecting...", e);
                            break;
                        },
                    }
                },
            }
        }
    }

    info!("Task ended.");
    Ok(())
}

/// Walk a status datagram and return `(OUTPUT_SSRC, FM_SNR_dB)` if both are
/// present. Returns `None` if either tag is missing or the buffer is malformed.
fn parse_status_tlv(buf: &[u8]) -> Option<(u32, f64)> {
    if buf.is_empty() || buf[0] != STATUS_TYPE {
        return None;
    }

    let mut i: usize = 1;
    let mut ssrc: Option<u32> = None;
    let mut snr: Option<f32> = None;

    while i < buf.len() {
        let tag = buf[i];
        i += 1;

        if tag == TAG_EOL {
            break;
        }

        if i >= buf.len() {
            return None;
        }

        let len_byte = buf[i];
        i += 1;

        let value_len: usize = if len_byte & 0x80 != 0 {
            let n = (len_byte & 0x7f) as usize;
            if n > 4 || i + n > buf.len() {
                return None;
            }
            let mut v: usize = 0;
            for _ in 0..n {
                v = (v << 8) | buf[i] as usize;
                i += 1;
            }
            v
        } else {
            len_byte as usize
        };

        if i + value_len > buf.len() {
            return None;
        }

        let value = &buf[i..i + value_len];
        i += value_len;

        match tag {
            TAG_OUTPUT_SSRC => ssrc = Some(decode_be_u32(value)),
            TAG_FM_SNR => snr = Some(f32::from_bits(decode_be_u32(value))),
            _ => {}
        }
    }

    match (ssrc, snr) {
        (Some(s), Some(n)) if n.is_finite() => Some((s, n as f64)),
        _ => None,
    }
}

/// Decode a leading-zero-suppressed big-endian integer (0..=4 bytes) into u32.
fn decode_be_u32(bytes: &[u8]) -> u32 {
    let mut v: u32 = 0;
    for &b in bytes {
        v = (v << 8) | b as u32;
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ssrc_and_snr() {
        // type=STATUS, then OUTPUT_SSRC=18 len=4 value=144390 (0x00023406),
        // then FM_SNR=66 len=4 value = bits of 22.5_f32, then EOL.
        let mut buf = vec![STATUS_TYPE];
        buf.push(TAG_OUTPUT_SSRC);
        buf.push(4);
        buf.extend_from_slice(&144_390u32.to_be_bytes());
        buf.push(TAG_FM_SNR);
        buf.push(4);
        buf.extend_from_slice(&22.5_f32.to_bits().to_be_bytes());
        buf.push(TAG_EOL);

        let parsed = parse_status_tlv(&buf).expect("should parse");
        assert_eq!(parsed.0, 144_390);
        assert!((parsed.1 - 22.5).abs() < 1e-6);
    }

    #[test]
    fn parses_short_ssrc_with_suppressed_leading_zeros() {
        // SSRC = 0x0102 encoded as length=2, 0x01 0x02
        let mut buf = vec![STATUS_TYPE];
        buf.push(TAG_OUTPUT_SSRC);
        buf.push(2);
        buf.extend_from_slice(&[0x01, 0x02]);
        buf.push(TAG_FM_SNR);
        buf.push(4);
        buf.extend_from_slice(&1.0_f32.to_bits().to_be_bytes());
        buf.push(TAG_EOL);

        let parsed = parse_status_tlv(&buf).expect("should parse");
        assert_eq!(parsed.0, 0x0102);
    }

    #[test]
    fn skips_unknown_tags() {
        let mut buf = vec![STATUS_TYPE];
        // Unknown tag with 3 bytes of payload
        buf.push(99);
        buf.push(3);
        buf.extend_from_slice(&[0xde, 0xad, 0xbe]);
        buf.push(TAG_OUTPUT_SSRC);
        buf.push(4);
        buf.extend_from_slice(&42u32.to_be_bytes());
        buf.push(TAG_FM_SNR);
        buf.push(4);
        buf.extend_from_slice(&5.0_f32.to_bits().to_be_bytes());
        buf.push(TAG_EOL);

        let parsed = parse_status_tlv(&buf).expect("should parse");
        assert_eq!(parsed.0, 42);
        assert!((parsed.1 - 5.0).abs() < 1e-6);
    }

    #[test]
    fn returns_none_without_required_tags() {
        let buf = vec![STATUS_TYPE, TAG_EOL];
        assert!(parse_status_tlv(&buf).is_none());
    }

    #[test]
    fn rejects_non_status_packet_type() {
        let buf = vec![1u8, TAG_EOL];
        assert!(parse_status_tlv(&buf).is_none());
    }

    #[tokio::test]
    async fn pop_oldest_returns_closed_cluster() {
        use crate::ka9q::pop_oldest_cluster;

        let map: SnrMap = Arc::new(RwLock::new(HashMap::new()));
        let stale = Instant::now() - Duration::from_millis(800);
        {
            let mut m = map.write().await;
            let dq = m.entry(1234).or_insert_with(VecDeque::new);
            // A single closed cluster (last sample > CLUSTER_GAP ago).
            let mut c = SnrCluster::new(10.0, stale - Duration::from_millis(200));
            c.extend(20.0, stale - Duration::from_millis(100));
            c.extend(30.0, stale);
            dq.push_back(c);
        }

        let popped = pop_oldest_cluster(&map, 1234).await.expect("should pop");
        assert_eq!(popped.count, 3);
        assert!((popped.min - 10.0).abs() < 1e-9);
        assert!((popped.max - 30.0).abs() < 1e-9);
        assert!((popped.avg() - 20.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn pop_oldest_skips_open_cluster() {
        use crate::ka9q::pop_oldest_cluster;

        let map: SnrMap = Arc::new(RwLock::new(HashMap::new()));
        let now = Instant::now();
        {
            let mut m = map.write().await;
            let dq = m.entry(2345).or_insert_with(VecDeque::new);
            // Single cluster whose newest sample is recent — still "open".
            dq.push_back(SnrCluster::new(5.0, now));
        }
        assert!(pop_oldest_cluster(&map, 2345).await.is_none());
    }

    #[tokio::test]
    async fn pop_oldest_pops_oldest_when_multiple_clusters_exist() {
        use crate::ka9q::pop_oldest_cluster;

        let map: SnrMap = Arc::new(RwLock::new(HashMap::new()));
        let now = Instant::now();
        {
            let mut m = map.write().await;
            let dq = m.entry(3456).or_insert_with(VecDeque::new);
            // Older closed cluster at the front, newer (open) cluster at the back.
            dq.push_back(SnrCluster::new(1.0, now - Duration::from_secs(2)));
            dq.push_back(SnrCluster::new(9.0, now));
        }
        let first = pop_oldest_cluster(&map, 3456).await.expect("front");
        assert!((first.min - 1.0).abs() < 1e-9);
        // The second is still open and should not be popped.
        assert!(pop_oldest_cluster(&map, 3456).await.is_none());
    }
}
