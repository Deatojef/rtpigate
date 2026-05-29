use tokio::sync::broadcast::{self};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use serde::{Serialize, Deserialize};
use serde_json::json;

use log::{info, warn, debug};

use crate::config::{Config, AppTelemetry, DataItem};
use crate::error::RtpigateError;
use crate::ka9q::Packet;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSEEvent {
    pub event: String,
    pub data: serde_json::Value,
}

pub async fn sse_task(data_channel: broadcast::Sender<DataItem>, sse_channel: broadcast::Sender<SSEEvent>, token: CancellationToken, _config: Arc<Config>) -> Result<(), RtpigateError> {

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
                        if let Err(_) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                            debug!("No SSE subscribers connected");
                        }
                    },

                    Ok(DataItem::SnrUpdate(update)) => {
                        let key = String::from("snr_update");
                        let thejson = json!(update);
                        if let Err(_) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                            debug!("No SSE subscribers connected");
                        }
                    },

                    Ok(DataItem::Tlm(telemetry)) => {
                        match telemetry {
                            AppTelemetry::PacketStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if let Err(_) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::AprsisStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if let Err(_) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    debug!("No SSE subscribers connected");
                                }
                            },
                            AppTelemetry::StationStatus(telem) => {
                                let key = telem.name.clone();
                                let thejson = json!(telem);
                                if let Err(_) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
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
