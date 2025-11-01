// main.rs
//#![allow(unused)]

use axum::{
    extract::{State, FromRef},
    response::{sse::Event, Sse},
    routing::{get},
    Router,
};
use tokio::{sync::{broadcast}, task::JoinSet};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::{sync::CancellationToken};
use tokio_stream::{Stream, wrappers::BroadcastStream, StreamExt};
use std::{sync::Arc, error::Error, convert::Infallible};

// for logging
use flexi_logger::{Logger};
use log::{info, warn, error, debug};

mod config;
use config::{Config, DataItem};

mod packet;
use packet::{rtp_listener};

mod aprsis;
use aprsis::{aprsis_task};

mod sse;
use sse::{sse_task, SSEEvent};


// for the axum application state
#[derive(Clone)]
struct AppState {
    sse_channel: broadcast::Sender<SSEEvent>,
}

impl FromRef<AppState> for broadcast::Sender<SSEEvent> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.sse_channel.clone()
    }
}


// Marks the `main` function as the entry point for a Tokio runtime.
#[tokio::main] 
async fn main() -> Result<(), Box<dyn Error>> {


    // initialize logging
    let _logger = Logger::try_with_str("info")? 
        .log_to_stdout()
        .format(|w, now, record| {

            // format the timestamp output
            let timestamp = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

            // use this as the module name
            let module_name = record.module_path().unwrap_or("<unknown>");

            let level = record.level();
            /*let colored_level = match level {
                log::Level::Error => Color::Red.paint(format!("{}", level)),
                log::Level::Warn => Color::Red.paint(format!("{}", level)),
                log::Level::Info => Color::Black.paint(format!("{}", level)),
                _ => Color::Black.paint(format!("{}", level)),
            };
            */

            // Write the formatted string to the output
            write!(
                w,
                "{} {} [{}] {}",
                timestamp,
                module_name,
                level,
                //colored_level,
                &record.args(),
            )
        })
        .start()?;



    // starting up the shields....
    info!("Application start");

    //------------- start:  read in the configuration ----------
    // the configuration file
    let config_path = "config.toml";

    // read in the config file
    info!("Attempting to read configuration file: {}", config_path);
    let config: Config = Config::from_file(config_path)?;

    debug!("Configuration: {:?}", config);

    // create a version of the configuration for sharing with other tasks
    let shared_config = Arc::new(config);

    //-------------- end:  reading configuration -------


    // create a JoinSet to collect the handles from tasks being started
    let mut task_set = JoinSet::new();

    // used to signal tasks that it's time to stop
    let cancel_token = CancellationToken::new();

    //
    // This is the conduit for sending data items to downstream tasks
    //
    // Create a broadcast channel with a buffer size of 128 for handling DataItem objects
    let (data_tx, _data_rx) = broadcast::channel::<DataItem>(128);

    //
    // This is the conduit for sending SSE events 
    //
    let (sse_tx, mut _sse_rx) = broadcast::channel::<SSEEvent>(16);


    // The expected number of tasks...incremented as tasks are spawned.
    let mut expected_tasks = 0;


    //#################
    // rtp_listener task
    //#################
    // clone the packet_tx Sender
    let rtp_tx_sender = data_tx.clone();

    // create a clone of the shared_config w/ Arc
    let rtp_config = Arc::clone(&shared_config);

    // create a clone of the cancellation token
    let rtp_token = cancel_token.clone();

    // spawn the database writer task
    task_set.spawn(async move {
        if let Err(e) = rtp_listener(rtp_tx_sender, rtp_token, rtp_config).await {
            error!("Unable to create RTP listener task: {}", e);
        }
    });

    // increment the expected number of tasks
    expected_tasks += 1;


    //#################
    // aprsis_task task
    //#################
    if let Some(aprsis) = shared_config.aprsis.enabled {

        if aprsis {
            // clone the gps_tx Sender
            let aprsis_tx_sender = data_tx.clone();

            // create a clone of the shared_config w/ Arc
            let aprsis_config = Arc::clone(&shared_config);

            // create a clone of the cancellation token
            let aprsis_token = cancel_token.clone();

            // spawn the database writer task
            task_set.spawn(async move {
                if let Err(e) = aprsis_task(aprsis_tx_sender, aprsis_token, aprsis_config).await {
                    error!("Unable to create aprsis task: {}", e);
                }
            });

            // increment the expected number of tasks
            expected_tasks += 1;
        }
    }


    //#################
    // sse_task task
    //#################
    // clone the data_tx Sender
    let sse_tx_sender = data_tx.clone();

    // clone the sse_tx Sender
    let sse_channel_tx_sender = sse_tx.clone();

    // create a clone of the shared_config w/ Arc
    let sse_config = Arc::clone(&shared_config);

    // create a clone of the cancellation token
    let sse_token = cancel_token.clone();

    // spawn the sse writer task
    task_set.spawn(async move {
        if let Err(e) = sse_task(sse_tx_sender, sse_channel_tx_sender, sse_token, sse_config).await {
            error!("Unable to create sse task: {}", e);
        }
    });

    // increment the expected number of tasks
    expected_tasks += 1;


    // if all expected tasks are running then continue with starting a listener for SSE
    if task_set.len() == expected_tasks {

        // the application state
        let app_state = AppState {
            sse_channel: sse_tx,
        };

        // create a new Router 
        let app = Router::new()
            .route("/sse", get(sse_handler))
            .with_state(app_state);

        // Relying upon webserver to redirect from public facing IP:port to
        // this localhost address for security concerns.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
        let addr = &listener.local_addr()?;

        // The axum http server.  
        let server = axum::serve(listener, app);

        info!("Listening on http://{}/sse", addr);

        let mut sigterm_stream = signal(SignalKind::terminate())?;
        let mut sigint_stream = signal(SignalKind::interrupt())?;

        // wait for either the server to shutdown or a signal 
        tokio::select! {

            // the http server
            _ = server => {
                info!("Server on http://{}/sse shutdown", addr); 
            },

            // signals 
            _ = sigint_stream.recv() => warn!("Received interrupt signal, application shutting down..."),
            _ = sigterm_stream.recv() => warn!("Received termination signal, application shutting down..."),
        }
    }

    // signal to all tasks that it's time to shutdown
    cancel_token.cancel();

    // wait for all tasks to finish
    task_set.join_all().await;

    info!("Done.");
    Ok(())
}


// sse_handler
async fn sse_handler(State(tx): State<broadcast::Sender<SSEEvent>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {

    // the channel to receive updates from.  That is SSEEvents are read from this channel and sent
    // to the browser
    let rx = tx.subscribe();

    // create the broadcast stream
    let stream = BroadcastStream::new(rx)
        .filter_map(|item| {
            match item {
                Ok(event) => Some(
                    Ok(Event::default().event(event.event).json_data(event.data).unwrap())
                    ),
                Err(_) => None,
            }
        });

    // return the new SSE stream
    Sse::new(stream)
}
