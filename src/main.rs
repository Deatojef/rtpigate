// main.rs

use axum::{
    Router,
    extract::{FromRef, State},
    response::{Json, Sse, sse::Event},
    routing::get,
};
use chrono::Local;
use std::{
    collections::VecDeque,
    convert::Infallible,
    error::Error,
    sync::{Arc, RwLock},
};
use tokio::signal::unix::{SignalKind, signal};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tokio_util::sync::CancellationToken;
use tower_http::services::ServeDir;

// for logging
use flexi_logger::Logger;
use log::{debug, error, info, warn};

mod error;

mod config;
use config::{APRSISPasscode, Config, DataItem, GpsFix, PositionSource, PublicConfig};

mod stream;
use stream::{RTPPacket, rtp_listener};

mod aprs_is;
use aprs_is::aprsis_task;

mod gpsd;
use gpsd::gpsd_task;

mod igate;

mod history;
use history::{HistoryStore, StatBucket};

mod sse;
use sse::{SSEEvent, sse_task};

// for the axum application state
#[derive(Clone)]
struct AppState {
    sse_channel: broadcast::Sender<SSEEvent>,
    public_config: Arc<RwLock<PublicConfig>>,
    sat_packet_log: Arc<RwLock<VecDeque<RTPPacket>>>,
    history: Arc<RwLock<HistoryStore>>,
}

impl FromRef<AppState> for broadcast::Sender<SSEEvent> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.sse_channel.clone()
    }
}

impl FromRef<AppState> for Arc<RwLock<PublicConfig>> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.public_config.clone()
    }
}

impl FromRef<AppState> for Arc<RwLock<VecDeque<RTPPacket>>> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.sat_packet_log.clone()
    }
}

impl FromRef<AppState> for Arc<RwLock<HistoryStore>> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.history.clone()
    }
}

// Marks the `main` function as the entry point for a Tokio runtime.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // resolve config file path: CLI arg > ./config.toml > /etc/rtpigate/config.toml
    let config_path = std::env::args().nth(1).unwrap_or_else(|| {
        if std::path::Path::new("config.toml").exists() {
            "config.toml".to_string()
        } else if std::path::Path::new("/etc/rtpigate/config.toml").exists() {
            "/etc/rtpigate/config.toml".to_string()
        } else {
            "config.toml".to_string() // will fail with a clear error
        }
    });
    let config: Config = Config::from_file(&config_path)?;

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
        error!(
            "Fix the above configuration errors in {} and restart.",
            config_path
        );
        std::process::exit(1);
    }

    // validate passcode if APRS-IS is enabled with igating or beaconing
    if config.aprsis.enabled == Some(true) {
        let needs_write =
            config.aprsis.igating == Some(true) || config.aprsis.beaconing == Some(true);
        if needs_write && !config.passcode_isvalid() {
            error!(
                "APRS-IS passcode is invalid for callsign {:?}. Igating and/or beaconing require a valid passcode.",
                config.station.callsign
            );
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
    let (sse_tx, mut _sse_rx) = broadcast::channel::<SSEEvent>(128);

    // The expected number of tasks...incremented as tasks are spawned.
    let mut expected_tasks = 0;

    //#################
    // 24h rolling log of satellite packets, shared between the RTP listener (writer)
    // and the /api/satellite-packets HTTP handler (reader).
    //#################
    let sat_packet_log: Arc<RwLock<VecDeque<RTPPacket>>> = Arc::new(RwLock::new(VecDeque::new()));

    //#################
    // 24h rolling history of merged packet/igating statistics, written by sse_task
    // and read by the /api/history HTTP handler.
    //#################
    let history_store: Arc<RwLock<HistoryStore>> = Arc::new(RwLock::new(HistoryStore::new()));

    //#################
    // latest GPS fix from gpsd, written by gpsd_task and read by aprsis_task to
    // source beacon position. Stays None when [location] source != gpsd.
    //#################
    let gps_state: Arc<RwLock<Option<GpsFix>>> = Arc::new(RwLock::new(None));

    //#################
    // rtp_listener task
    //#################
    let rtp_tx_sender = data_tx.clone();
    let rtp_config = Arc::clone(&shared_config);
    let rtp_token = cancel_token.clone();
    let rtp_sat_log = Arc::clone(&sat_packet_log);

    task_set.spawn(async move {
        if let Err(e) = rtp_listener(rtp_tx_sender, rtp_token, rtp_config, rtp_sat_log).await {
            error!("Unable to create RTP listener task: {}", e);
        }
    });

    expected_tasks += 1;

    //#################
    // aprsis_task task
    //#################
    if shared_config.aprsis.enabled == Some(true) {
        let aprsis_tx_sender = data_tx.clone();
        let aprsis_config = Arc::clone(&shared_config);
        let aprsis_token = cancel_token.clone();
        let aprsis_gps_state = Arc::clone(&gps_state);

        task_set.spawn(async move {
            if let Err(e) = aprsis_task(
                aprsis_tx_sender,
                aprsis_token,
                aprsis_config,
                aprsis_gps_state,
            )
            .await
            {
                error!("Unable to create aprsis task: {}", e);
            }
        });

        expected_tasks += 1;
    }

    //#################
    // gpsd_task task — only spawned when GPSD is the position source
    //#################
    if shared_config.location.source == PositionSource::Gpsd {
        let gpsd_tx_sender = data_tx.clone();
        let gpsd_config = Arc::clone(&shared_config);
        let gpsd_token = cancel_token.clone();
        let gpsd_gps_state = Arc::clone(&gps_state);

        task_set.spawn(async move {
            if let Err(e) = gpsd_task(gpsd_tx_sender, gpsd_token, gpsd_config, gpsd_gps_state).await
            {
                error!("Unable to create gpsd task: {}", e);
            }
        });

        expected_tasks += 1;
    }

    //#################
    // sse_task task
    //#################
    let sse_tx_sender = data_tx.clone();
    let sse_channel_tx_sender = sse_tx.clone();
    let sse_config = Arc::clone(&shared_config);
    let sse_token = cancel_token.clone();
    let sse_history = Arc::clone(&history_store);

    task_set.spawn(async move {
        if let Err(e) = sse_task(
            sse_tx_sender,
            sse_channel_tx_sender,
            sse_token,
            sse_config,
            sse_history,
        )
        .await
        {
            error!("Unable to create sse task: {}", e);
        }
    });

    expected_tasks += 1;

    // if all expected tasks are running then continue with starting a listener for SSE
    if task_set.len() == expected_tasks {
        // the application state
        let mut public_config = shared_config.to_public();
        let started_at = Local::now();
        public_config.started_at = Some(started_at);
        let shared_public_config = Arc::new(RwLock::new(public_config));
        let sse_tx_for_reload = sse_tx.clone();
        let app_state = AppState {
            sse_channel: sse_tx,
            public_config: shared_public_config.clone(),
            sat_packet_log: Arc::clone(&sat_packet_log),
            history: Arc::clone(&history_store),
        };

        // resolve frontend assets path
        let frontend_dir = shared_config
            .http
            .as_ref()
            .and_then(|h| h.frontend.as_deref())
            .unwrap_or("frontend");
        let assets_dir = format!("{}/assets", frontend_dir);

        info!("Frontend directory: {}", frontend_dir);

        // create a new Router
        let app = Router::new()
            .route("/api/sse", get(sse_handler))
            .route("/api/config", get(config_handler))
            .route("/api/satellite-packets", get(satellite_packets_handler))
            .route("/api/history", get(history_handler))
            .nest_service("/assets", ServeDir::new(&assets_dir))
            .fallback_service(ServeDir::new(frontend_dir).append_index_html_on_directories(true))
            .with_state(app_state);

        // HTTP listen address (configurable, defaults to localhost for security)
        let listen_addr = shared_config
            .http
            .as_ref()
            .and_then(|h| h.listen.as_deref())
            .unwrap_or("127.0.0.1:3000");
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        let addr = &listener.local_addr()?;

        // The axum http server (converted to future and pinned for select loop)
        let server = axum::serve(listener, app).into_future();
        tokio::pin!(server);

        info!("Listening on http://{}/api/sse", addr);

        let mut sigterm_stream = signal(SignalKind::terminate())?;
        let mut sigint_stream = signal(SignalKind::interrupt())?;
        let mut sighup_stream = signal(SignalKind::hangup())?;

        // wait for either the server to shutdown, a signal, or a task exit
        loop {
            tokio::select! {

                // the http server
                _ = &mut server => {
                    info!("Server on http://{}/api/sse shutdown", addr);
                    break;
                },

                // shutdown signals
                _ = sigint_stream.recv() => {
                    warn!("Received interrupt signal, application shutting down...");
                    break;
                },
                _ = sigterm_stream.recv() => {
                    warn!("Received termination signal, application shutting down...");
                    break;
                },

                // SIGHUP: reload configuration
                _ = sighup_stream.recv() => {
                    info!("Received SIGHUP, reloading configuration from {}...", config_path);
                    match Config::from_file(&config_path) {
                        Ok(new_config) => {
                            let errors = new_config.validate();
                            if !errors.is_empty() {
                                for err in &errors {
                                    error!("Config reload error: {}", err);
                                }
                                warn!("Config reload failed validation, keeping current config.");
                            } else {
                                let mut new_public = new_config.to_public();
                                new_public.started_at = Some(started_at);
                                match shared_public_config.write() {
                                    Ok(mut cfg) => {
                                        *cfg = new_public.clone();
                                        info!("Configuration reloaded successfully.");
                                        // push config update to connected browsers via SSE
                                        let config_json = serde_json::json!(new_public);
                                        let _ = sse_tx_for_reload.send(SSEEvent {
                                            event: String::from("config"),
                                            data: config_json,
                                        });
                                    },
                                    Err(e) => error!("Failed to update config: {}", e),
                                }
                            }
                        },
                        Err(e) => {
                            error!("Failed to read {}: {}. Keeping current config.", config_path, e);
                        }
                    }
                },

                // monitor background task health
                result = task_set.join_next() => {
                    if let Some(result) = result {
                        match result {
                            Ok(()) => warn!("A background task exited unexpectedly"),
                            Err(e) => error!("A background task panicked: {}", e),
                        }
                    }
                    break;
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
async fn config_handler(State(config): State<Arc<RwLock<PublicConfig>>>) -> Json<PublicConfig> {
    let cfg = config.read().unwrap().clone();
    Json(cfg)
}

// satellite_packets_handler - returns the 24h rolling log of satellite packets,
// newest-first.
async fn satellite_packets_handler(
    State(log): State<Arc<RwLock<VecDeque<RTPPacket>>>>,
) -> Json<Vec<RTPPacket>> {
    let snapshot: Vec<RTPPacket> = match log.read() {
        Ok(guard) => guard.iter().rev().cloned().collect(),
        Err(_) => Vec::new(),
    };
    Json(snapshot)
}

// history_handler - returns the 24h rolling history of merged packet/igating
// statistics as 15s buckets, oldest-first, for seeding the activity chart on load.
async fn history_handler(
    State(history): State<Arc<RwLock<HistoryStore>>>,
) -> Json<Vec<StatBucket>> {
    let snapshot = match history.read() {
        Ok(guard) => guard.snapshot(),
        Err(_) => Vec::new(),
    };
    Json(snapshot)
}

// sse_handler
async fn sse_handler(
    State(tx): State<broadcast::Sender<SSEEvent>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // the channel to receive updates from
    let rx = tx.subscribe();

    // create the broadcast stream
    let stream = BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(event) => Some(Ok(Event::default()
            .event(event.event)
            .json_data(event.data)
            .unwrap())),
        Err(_) => None,
    });

    // return the new SSE stream
    Sse::new(stream)
}
