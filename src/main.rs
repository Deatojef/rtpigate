// main.rs

use axum::{
    extract::{State, FromRef},
    response::{sse::Event, Json, Sse},
    routing::get,
    Router,
};
use tower_http::services::ServeDir;
use tokio::{sync::broadcast, task::JoinSet};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tokio_stream::{Stream, wrappers::BroadcastStream, StreamExt};
use std::{sync::Arc, error::Error, convert::Infallible};
use chrono::Local;

// for logging
use flexi_logger::Logger;
use log::{info, warn, error, debug};

mod config;
use config::{Config, DataItem, PublicConfig, APRSISPasscode};

mod ka9q;
use ka9q::rtp_listener;

mod aprs_is;
use aprs_is::aprsis_task;

mod igate;

mod sse;
use sse::{sse_task, SSEEvent};


// for the axum application state
#[derive(Clone)]
struct AppState {
    sse_channel: broadcast::Sender<SSEEvent>,
    public_config: PublicConfig,
}

impl FromRef<AppState> for broadcast::Sender<SSEEvent> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.sse_channel.clone()
    }
}

impl FromRef<AppState> for PublicConfig {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.public_config.clone()
    }
}


// Marks the `main` function as the entry point for a Tokio runtime.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {


    // read configuration first so we can set the log level
    let config_path = "config.toml";
    let config: Config = Config::from_file(config_path)?;

    // set log level based on verbose config setting
    let log_level = match config.station.verbose {
        Some(true) => "debug",
        _ => "info",
    };

    // initialize logging
    let _logger = Logger::try_with_str(log_level)?
        .log_to_stdout()
        .format(|w, now, record| {
            let timestamp = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            let module_name = record.module_path().unwrap_or("<unknown>");
            let level = record.level();

            write!(
                w,
                "{} {} [{}] {}",
                timestamp,
                module_name,
                level,
                &record.args(),
            )
        })
        .start()?;

    info!("Application start");
    info!("Configuration file: {}", config_path);
    if log_level == "debug" {
        info!("Verbose logging enabled");
    }
    debug!("Configuration: {:?}", config);

    // validate configuration
    let validation_errors = config.validate();
    if !validation_errors.is_empty() {
        for err in &validation_errors {
            error!("Config error: {}", err);
        }
        error!("Fix the above configuration errors in {} and restart.", config_path);
        std::process::exit(1);
    }

    // validate passcode if APRS-IS is enabled with igating or beaconing
    if config.aprsis.enabled == Some(true) {
        let needs_write = config.aprsis.igating == Some(true) || config.aprsis.beaconing == Some(true);
        if needs_write && !config.passcode_isvalid() {
            error!("APRS-IS passcode is invalid for callsign {:?}. Igating and/or beaconing require a valid passcode.", config.station.callsign);
            std::process::exit(1);
        }
    }

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
    let rtp_tx_sender = data_tx.clone();
    let rtp_config = Arc::clone(&shared_config);
    let rtp_token = cancel_token.clone();

    task_set.spawn(async move {
        if let Err(e) = rtp_listener(rtp_tx_sender, rtp_token, rtp_config).await {
            error!("Unable to create RTP listener task: {}", e);
        }
    });

    expected_tasks += 1;


    //#################
    // aprsis_task task
    //#################
    if let Some(aprsis) = shared_config.aprsis.enabled {

        if aprsis {
            let aprsis_tx_sender = data_tx.clone();
            let aprsis_config = Arc::clone(&shared_config);
            let aprsis_token = cancel_token.clone();

            task_set.spawn(async move {
                if let Err(e) = aprsis_task(aprsis_tx_sender, aprsis_token, aprsis_config).await {
                    error!("Unable to create aprsis task: {}", e);
                }
            });

            expected_tasks += 1;
        }
    }


    //#################
    // sse_task task
    //#################
    let sse_tx_sender = data_tx.clone();
    let sse_channel_tx_sender = sse_tx.clone();
    let sse_config = Arc::clone(&shared_config);
    let sse_token = cancel_token.clone();

    task_set.spawn(async move {
        if let Err(e) = sse_task(sse_tx_sender, sse_channel_tx_sender, sse_token, sse_config).await {
            error!("Unable to create sse task: {}", e);
        }
    });

    expected_tasks += 1;


    // if all expected tasks are running then continue with starting a listener for SSE
    if task_set.len() == expected_tasks {

        // the application state
        let mut public_config = shared_config.to_public();
        public_config.started_at = Some(Local::now());
        let app_state = AppState {
            sse_channel: sse_tx,
            public_config,
        };

        // create a new Router
        let app = Router::new()
            .route("/api/sse", get(sse_handler))
            .route("/api/config", get(config_handler))
            .nest_service("/assets", ServeDir::new("frontend/assets"))
            .fallback_service(ServeDir::new("frontend").append_index_html_on_directories(true))
            .with_state(app_state);

        // HTTP listen address (configurable, defaults to localhost for security)
        let listen_addr = shared_config.http.as_ref()
            .and_then(|h| h.listen.as_deref())
            .unwrap_or("127.0.0.1:3000");
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        let addr = &listener.local_addr()?;

        // The axum http server.
        let server = axum::serve(listener, app);

        info!("Listening on http://{}/api/sse", addr);

        let mut sigterm_stream = signal(SignalKind::terminate())?;
        let mut sigint_stream = signal(SignalKind::interrupt())?;

        // wait for either the server to shutdown, a signal, or a task exit
        tokio::select! {

            // the http server
            _ = server => {
                info!("Server on http://{}/api/sse shutdown", addr);
            },

            // signals
            _ = sigint_stream.recv() => warn!("Received interrupt signal, application shutting down..."),
            _ = sigterm_stream.recv() => warn!("Received termination signal, application shutting down..."),

            // monitor background task health
            result = task_set.join_next() => {
                if let Some(result) = result {
                    match result {
                        Ok(()) => warn!("A background task exited unexpectedly"),
                        Err(e) => error!("A background task panicked: {}", e),
                    }
                }
            }
        }
    }

    // signal to all tasks that it's time to shutdown
    cancel_token.cancel();

    // wait for all tasks to finish
    task_set.join_all().await;

    info!("Done.");
    Ok(())
}


// config_handler - returns sanitized config as JSON (no passcode)
async fn config_handler(State(config): State<PublicConfig>) -> Json<PublicConfig> {
    Json(config)
}

// sse_handler
async fn sse_handler(State(tx): State<broadcast::Sender<SSEEvent>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {

    // the channel to receive updates from
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
