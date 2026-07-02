use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::{self};
use tokio_util::sync::CancellationToken;

use log::{debug, info, warn};

use crate::config::{AppTelemetry, Config, DataItem};
use crate::error::RtpigateError;
use crate::history::{HistoryStore, StatBucket};
use crate::store::Store;
use crate::stream::Packet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSEEvent {
    pub event: String,
    pub data: serde_json::Value,
}

pub async fn sse_task(
    data_channel: broadcast::Sender<DataItem>,
    sse_channel: broadcast::Sender<SSEEvent>,
    token: CancellationToken,
    _config: Arc<Config>,
    history: Arc<RwLock<HistoryStore>>,
    store: Arc<Store>,
) -> Result<(), RtpigateError> {
    info!("Started");

    // subscribe to the channels
    let mut data_stream = data_channel.subscribe();

    // loop until an error, thread was cancelled, or new configuration info was received.
    loop {
        tokio::select! {

            // was this thread canceled?
            _ = token.cancelled() => {
                break;
            },

            // check for updates from the data channel
            message = data_stream.recv() => {
                match message {
                    Ok(DataItem::Pkt(Packet::RTP(p))) => {
                        let key = String::from("rfpacket");
                        let thejson = json!(p);
                        if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                            debug!("No SSE subscribers connected");
                        }
                    },

                    Ok(DataItem::Tlm(telemetry)) => {
                        match telemetry {
                            AppTelemetry::PacketStatus(telem) => {
                                // feed the rolling history store before forwarding,
                                // then mirror the touched/pruned buckets to disk.
                                let (touched, pruned) = match history.write() {
                                    Ok(mut hist) => {
                                        let touched = hist.update_from_packet(&telem);
                                        let pruned = hist.prune();
                                        (hist.buckets_for(&touched), pruned)
                                    }
                                    Err(_) => (Vec::new(), Vec::new()),
                                };
                                persist_history(&store, &touched, &pruned);
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::AprsisStatus(telem) => {
                                // feed the rolling history store before forwarding,
                                // then mirror the touched/pruned buckets to disk.
                                let (touched, pruned) = match history.write() {
                                    Ok(mut hist) => {
                                        let touched = hist.update_from_aprsis(&telem);
                                        let pruned = hist.prune();
                                        (hist.buckets_for(&touched), pruned)
                                    }
                                    Err(_) => (Vec::new(), Vec::new()),
                                };
                                persist_history(&store, &touched, &pruned);
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::SlicerStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::StationStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::GpsStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if sse_channel.send(SSEEvent { event: key, data: thejson }).is_err() {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                        }
                    },

                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("SSE data channel lagged, skipped {} messages", n);
                    },

                    _ => (),
                }

            } // message = data_stream.recv() => {
        }
    }

    info!("Task ended.");
    Ok(())
}

// Mirror the just-merged history buckets to the persistent store. Persistence
// failures are logged but never fatal — the in-memory store remains authoritative
// for live reads, and the next tick retries.
fn persist_history(store: &Store, touched: &[StatBucket], pruned: &[i64]) {
    if let Err(e) = store.upsert_buckets(touched) {
        warn!("Failed to persist history buckets: {}", e);
    }
    if let Err(e) = store.delete_buckets(pruned) {
        warn!("Failed to delete pruned history buckets: {}", e);
    }
}
