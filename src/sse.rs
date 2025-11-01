// dbwriter.rs
//#![allow(unused)]
use tokio::{sync::broadcast};
use std::{self, sync::Arc};
use tokio_util::sync::{CancellationToken};
use serde::{Serialize, Deserialize};
use serde_json::{json};

// for logging
use log::{info, error};

use crate::config::{Config, AppTelemetry, DataItem};
use crate::packet::{Packet};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSEEvent {
    pub event: String,
    pub data: serde_json::Value,
}

pub async fn sse_task(data_channel: broadcast::Sender<DataItem>, sse_channel: broadcast::Sender<SSEEvent>, token: CancellationToken, _config: Arc<Config>) -> Result<(), Box<dyn std::error::Error>> {

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
                    Ok(DataItem::Pkt(p)) => {
                        match p {
                            Packet::Inet(p) => {

                                // the key to be used for saving this statistics JSON too
                                let key = String::from("inetpacket");

                                // serialize the incoming struct as json
                                let thejson = json!(p);

                                // Send this sse event to the channel
                                if let Err(e) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    error!("Failed to send SSE data to channel (receiver likely dropped): {}", e);

                                    // If sending fails, it means the receiver is gone, so propagate the error
                                    return Err(e.into());
                                }
                            },
                            Packet::RTP(p) => {

                                // the key to be used for saving this statistics JSON too
                                let key = String::from("rfpacket");

                                // serialize the incoming struct as json
                                let thejson = json!(p);

                                // Send this sse event to the channel
                                if let Err(e) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    error!("Failed to send SSE data to channel (receiver likely dropped): {}", e);

                                    // If sending fails, it means the receiver is gone, so propagate the error
                                    return Err(e.into());
                                }
                            },
                        }
                    },

                    Ok(DataItem::Tlm(telemetry)) => {
                        match telemetry {
                            AppTelemetry::PacketStatus(telem) => {

                                // the key to be used for saving this statistics JSON too
                                let key = telem.name.clone();

                                // serialize the incoming struct as json
                                let thejson = json!(telem);

                                // Send this sse event to the channel
                                if let Err(e) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    error!("Failed to send SSE data to channel (receiver likely dropped): {}", e);

                                    // If sending fails, it means the receiver is gone, so propagate the error
                                    return Err(e.into());
                                }
                            },
                            AppTelemetry::AprsisStatus(telem) => {

                                // the key to be used for saving this statistics JSON too
                                let key = telem.name.clone();

                                // serialize the incoming struct as json
                                let thejson = json!(telem);

                                // Send this sse event to the channel
                                if let Err(e) = sse_channel.send(SSEEvent { event: key, data: thejson }) {
                                    error!("Failed to send SSE data to channel (receiver likely dropped): {}", e);

                                    // If sending fails, it means the receiver is gone, so propagate the error
                                    return Err(e.into());
                                }
                            },
                        }
                    },

                    _ => (),
                }

            } // message = data_stream.recv() => {
        }
    }


    info!("Task ended.");
    Ok(())
}
