mod book;
mod market;
mod transport;
mod verify;

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::serve::ListenerExt;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use market::{Command, Event, MarketId, Session};
use transport::KafkaRun;

const KAFKA_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MAX_SESSIONS: usize = 16;
const MAX_REPLAYS: usize = 8;
const RUN_COUNTER_MASK: u64 = u16::MAX as u64;

struct App {
    broker: Option<String>,
    next_id: AtomicU64,
    lifecycle: Mutex<()>,
    sessions: Mutex<BTreeMap<u64, Arc<Mutex<Run>>>>,
    replays: Mutex<BTreeMap<u64, Arc<Mutex<ReplayRun>>>>,
}

impl App {
    fn allocate_run_id(&self) -> Result<u64, ApiError> {
        self.next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                (id & RUN_COUNTER_MASK != RUN_COUNTER_MASK).then_some(id + 1)
            })
            .map_err(|_| {
                ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Run IDs exhausted. Restart the server.".into(),
                )
            })
    }
}

struct ReplayRun {
    kafka: Option<KafkaRun>,
    revision: u64,
    clock_ms: u64,
    last_reply: Value,
    fault: Option<String>,
    last_events: Option<Vec<Event>>,
    closed: bool,
}

struct Run {
    id: u64,
    revision: u64,
    sim: Session,
    kafka: Option<KafkaRun>,
    fault: Option<String>,
    recent: VecDeque<Value>,
    clock_ms: u64,
    previous: Option<PreviousSnapshot>,
    last_command: Option<AcceptedCommand>,
    closed: bool,
}

struct PreviousSnapshot {
    revision: u64,
    sim: Session,
    recent: VecDeque<Value>,
}

struct AcceptedCommand {
    revision: u64,
    fingerprint: String,
    reply: Value,
}

impl Run {
    fn snapshot(&self) -> Value {
        let mut state = self.sim.snapshot();
        self.decorate_snapshot(&mut state, self.revision, &self.recent);
        state
    }

    fn decorate_snapshot(&self, state: &mut Value, revision: u64, recent: &VecDeque<Value>) {
        state["id"] = json!(self.id);
        state["revision"] = json!(revision);
        state["transport"] = json!(if self.kafka.is_some() {
            "kafka"
        } else {
            "local"
        });
        state["topics"] = json!(self.kafka.as_ref().map(|kafka| {
            json!({"market": kafka.market_topic, "private": kafka.private_topic})
        }));
        state["fault"] = json!(self.fault);
        state["recent"] = json!(recent);
    }

    async fn execute(
        &mut self,
        command: Command,
        events_only: bool,
        fingerprint: Option<String>,
    ) -> ApiResult {
        let started = std::time::Instant::now();
        let events = self
            .sim
            .plan(command)
            .map_err(|error| ApiError(StatusCode::UNPROCESSABLE_ENTITY, error))?;
        let plan_ms = started.elapsed().as_secs_f64() * 1000.0;
        let delivery_started = std::time::Instant::now();
        let delivered = match self.kafka.as_ref() {
            Some(kafka) => tokio::time::timeout(KAFKA_TIMEOUT, kafka.deliver_measured(events))
                .await
                .map_err(|_| "Kafka delivery timed out.".to_string())
                .and_then(|result| result),
            None => Ok(transport::Delivery {
                events,
                timings: Default::default(),
            }),
        };
        let delivered = match delivered {
            Ok(delivered) => delivered,
            Err(error) => {
                let fault = format!("{error} Reset this session to reconnect.");
                self.fault = Some(fault.clone());
                return Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, fault));
            }
        };
        let kafka_timings = delivered.timings;
        let consumed = delivered.events;
        let delivery_ms = delivery_started.elapsed().as_secs_f64() * 1000.0;
        let market_records = consumed.iter().filter(|event| !event.is_private()).count();
        let private_records = consumed.len() - market_records;
        let clock_ms = consumed
            .last()
            .map_or(self.clock_ms, |event| event.clock_ms);
        let trace_started = std::time::Instant::now();
        let trace = serde_json::to_value(&consumed)
            .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        let trace_ms = trace_started.elapsed().as_secs_f64() * 1000.0;
        let apply_started = std::time::Instant::now();
        let mut candidate = self.sim.clone();
        if let Err(error) = candidate.apply(consumed) {
            self.fault = Some(error.clone());
            return Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, error));
        }
        let previous = std::mem::replace(&mut self.sim, candidate);
        let apply_ms = apply_started.elapsed().as_secs_f64() * 1000.0;
        let checkpoint_started = std::time::Instant::now();
        self.previous = events_only.then(|| PreviousSnapshot {
            revision: self.revision,
            sim: previous,
            recent: self.recent.clone(),
        });
        let checkpoint_copy_ms = checkpoint_started.elapsed().as_secs_f64() * 1000.0;
        self.revision += 1;
        self.clock_ms = clock_ms;
        if self.recent.len() == 20 {
            self.recent.pop_front();
        }
        let snapshot_started = std::time::Instant::now();
        let mut state = if events_only {
            json!({
                "id": self.id,
                "revision": self.revision,
                "transport": if self.kafka.is_some() { "kafka" } else { "local" },
                "topics": self.kafka.as_ref().map(|kafka| json!({"market": kafka.market_topic, "private": kafka.private_topic})),
                "fault": self.fault,
                "clock_ms": self.clock_ms,
                "stage": if self.clock_ms >= 86_400_000 { "complete" } else { "running" },
                "events_only": true,
            })
        } else {
            self.snapshot()
        };
        let snapshot_ms = if events_only {
            0.0
        } else {
            snapshot_started.elapsed().as_secs_f64() * 1000.0
        };
        self.recent.push_back(json!({
            "clock_ms":clock_ms,"market_records":market_records,
            "private_records":private_records,"event":format!("{market_records} market / {private_records} private events"),
            "plan_ms":plan_ms,"delivery_ms":delivery_ms,"trace_ms":trace_ms,
            "apply_ms":apply_ms,"snapshot_ms":snapshot_ms,"checkpoint_copy_ms":checkpoint_copy_ms,"kafka":kafka_timings,
            "server_ms":started.elapsed().as_secs_f64() * 1000.0,
        }));
        state["recent"] = json!(self.recent);
        state["events"] = trace;
        self.last_command = fingerprint.map(|fingerprint| AcceptedCommand {
            revision: self.revision - 1,
            fingerprint,
            reply: state.clone(),
        });
        Ok(json_response(state))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Transport {
    Local,
    Kafka,
}

#[derive(Deserialize)]
struct Start {
    seed: u64,
    transport: Transport,
    #[serde(default)]
    scenario: Scenario,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    #[default]
    Reference,
    CarryDemo,
    QueueDemo,
}

#[derive(Deserialize)]
struct Step {
    revision: u64,
    seconds: Option<u32>,
    milliseconds: Option<u32>,
    #[serde(default)]
    events_only: bool,
}

#[derive(Default, Deserialize)]
struct CurrentQuery {
    revision: Option<u64>,
}

#[derive(Deserialize)]
struct Place {
    revision: u64,
    lots: u32,
    spot_limit_cents: i64,
    future_limit_cents: i64,
}

#[derive(Deserialize)]
struct Enter {
    #[serde(flatten)]
    pair: Place,
    cross_market: MarketId,
}

#[derive(Deserialize)]
struct Revision {
    revision: u64,
}

struct ApiError(StatusCode, String);
type ApiResult = Result<Response, ApiError>;

fn json_response(value: Value) -> Response {
    let started = std::time::Instant::now();
    let mut response = Json(value).into_response();
    response.headers_mut().insert(
        "x-response-encode-ms",
        format!("{:.4}", started.elapsed().as_secs_f64() * 1000.0)
            .parse()
            .unwrap(),
    );
    response
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}

async fn config(State(app): State<Arc<App>>) -> Json<Value> {
    Json(json!({
        "kafka_available":app.broker.is_some(),
        "default_transport":if app.broker.is_some() {"kafka"} else {"local"},
        "default_seed":73,
    }))
}

async fn create_transport(
    app: &App,
    transport: Transport,
    id: u64,
) -> Result<Option<KafkaRun>, ApiError> {
    match transport {
        Transport::Local => Ok(None),
        Transport::Kafka => {
            let broker = app.broker.as_deref().ok_or_else(|| {
                ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Kafka is not configured.".into(),
                )
            })?;
            KafkaRun::new(broker, id)
                .await
                .map(Some)
                .map_err(|error| ApiError(StatusCode::SERVICE_UNAVAILABLE, error))
        }
    }
}

fn background_task_error(error: tokio::task::JoinError) -> ApiError {
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Request failed: {error}"),
    )
}

async fn cleanup_after_failed_start(kafka: Option<&KafkaRun>, mut error: ApiError) -> ApiError {
    if let Some(kafka) = kafka
        && let Err(cleanup) = kafka.cleanup_topics().await
    {
        error
            .1
            .push_str(&format!(" Candidate cleanup failed: {cleanup}"));
    }
    error
}

async fn start(State(app): State<Arc<App>>, Json(input): Json<Start>) -> ApiResult {
    if input.seed > MAX_SAFE_INTEGER {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Seed is too large.".into(),
        ));
    }
    // Own provisioning and cleanup even if the HTTP request is dropped.
    tokio::spawn(start_session(app, input))
        .await
        .map_err(background_task_error)?
}

async fn start_session(app: Arc<App>, input: Start) -> ApiResult {
    let _lifecycle = app.lifecycle.lock().await;
    let id = app.allocate_run_id()?;
    let kafka = create_transport(&app, input.transport, id).await?;
    let mut run = Run {
        id,
        revision: 0,
        sim: match input.scenario {
            Scenario::Reference => Session::new(input.seed),
            Scenario::CarryDemo => Session::carry_demo(input.seed),
            Scenario::QueueDemo => Session::queue_demo(input.seed),
        },
        kafka,
        fault: None,
        recent: VecDeque::new(),
        clock_ms: 0,
        previous: None,
        last_command: None,
        closed: false,
    };
    if let Err(error) = run.execute(Command::Initialize, false, None).await {
        return Err(cleanup_after_failed_start(run.kafka.as_ref(), error).await);
    }
    let at_capacity = app.sessions.lock().await.len() >= MAX_SESSIONS;
    if at_capacity && let Err(error) = evict_oldest_session(&app).await {
        return Err(cleanup_after_failed_start(
            run.kafka.as_ref(),
            ApiError(StatusCode::SERVICE_UNAVAILABLE, error),
        )
        .await);
    }
    let state = run.snapshot();
    let mut sessions = app.sessions.lock().await;
    sessions.insert(id, Arc::new(Mutex::new(run)));
    Ok(json_response(state))
}

async fn find(app: &App, id: u64) -> Result<Arc<Mutex<Run>>, ApiError> {
    app.sessions.lock().await.get(&id).cloned().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_FOUND,
            "Session expired. Start a new session.".into(),
        )
    })
}

async fn cleanup_run(run: Arc<Mutex<Run>>) -> Option<String> {
    let run = run.lock().await;
    if let Some(kafka) = &run.kafka {
        return kafka.cleanup_topics().await.err();
    }
    None
}

async fn evict_oldest_session(app: &App) -> Result<(), String> {
    let (id, entry) = app
        .sessions
        .lock()
        .await
        .first_key_value()
        .map(|(id, run)| (*id, run.clone()))
        .ok_or_else(|| "Session capacity changed during eviction.".to_owned())?;
    entry.lock().await.closed = true;
    if let Some(error) = cleanup_run(entry.clone()).await {
        return Err(error);
    }
    let mut sessions = app.sessions.lock().await;
    if sessions
        .get(&id)
        .is_some_and(|current| Arc::ptr_eq(current, &entry))
    {
        sessions.remove(&id);
    }
    Ok(())
}

async fn close_session(State(app): State<Arc<App>>, Path(id): Path<u64>) -> ApiResult {
    let _lifecycle = app.lifecycle.lock().await;
    let run = app
        .sessions
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Session expired.".into()))?;
    run.lock().await.closed = true;
    if let Some(error) = cleanup_run(run.clone()).await {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"closed":false,"id":id,"cleanup_error":error})),
        )
            .into_response());
    }
    let mut sessions = app.sessions.lock().await;
    if sessions
        .get(&id)
        .is_some_and(|current| Arc::ptr_eq(current, &run))
    {
        sessions.remove(&id);
    }
    Ok(json_response(
        json!({"closed":true,"id":id,"cleanup_error":null}),
    ))
}

async fn cleanup_replay(run: Arc<Mutex<ReplayRun>>) -> Option<String> {
    let run = run.lock().await;
    if let Some(kafka) = &run.kafka {
        return kafka.cleanup_topics().await.err();
    }
    None
}

async fn evict_oldest_replay(app: &App) -> Result<(), String> {
    let (id, entry) = app
        .replays
        .lock()
        .await
        .first_key_value()
        .map(|(id, run)| (*id, run.clone()))
        .ok_or_else(|| "Replay capacity changed during eviction.".to_owned())?;
    entry.lock().await.closed = true;
    if let Some(error) = cleanup_replay(entry.clone()).await {
        return Err(error);
    }
    let mut replays = app.replays.lock().await;
    if replays
        .get(&id)
        .is_some_and(|current| Arc::ptr_eq(current, &entry))
    {
        replays.remove(&id);
    }
    Ok(())
}

async fn close_replay(State(app): State<Arc<App>>, Path(id): Path<u64>) -> ApiResult {
    let _lifecycle = app.lifecycle.lock().await;
    let run = app
        .replays
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Replay expired.".into()))?;
    run.lock().await.closed = true;
    if let Some(error) = cleanup_replay(run.clone()).await {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"closed":false,"id":id,"cleanup_error":error})),
        )
            .into_response());
    }
    let mut replays = app.replays.lock().await;
    if replays
        .get(&id)
        .is_some_and(|current| Arc::ptr_eq(current, &run))
    {
        replays.remove(&id);
    }
    Ok(json_response(
        json!({"closed":true,"id":id,"cleanup_error":null}),
    ))
}

async fn current(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Query(query): Query<CurrentQuery>,
) -> ApiResult {
    let run = find(&app, id).await?;
    let run = run.lock().await;
    if run.closed {
        return Err(ApiError(
            StatusCode::GONE,
            "Session is closing or closed.".into(),
        ));
    }
    let requested_revision = query.revision.unwrap_or(run.revision);
    if requested_revision == run.revision {
        return Ok(json_response(run.snapshot()));
    }
    if let Some(previous) = &run.previous
        && requested_revision == previous.revision
    {
        let mut state = previous.sim.snapshot();
        run.decorate_snapshot(&mut state, previous.revision, &previous.recent);
        return Ok(json_response(state));
    }
    Err(ApiError(
        StatusCode::CONFLICT,
        format!("Revision {requested_revision} is no longer available."),
    ))
}

async fn mutate(
    app: &App,
    id: u64,
    revision: u64,
    command: Command,
    events_only: bool,
    fingerprint: String,
) -> ApiResult {
    let run = find(app, id).await?;
    // Accepted work survives a client disconnect; only an exact latest retry reads the cache.
    tokio::spawn(async move {
        let mut run = run.lock().await;
        if run.closed {
            return Err(ApiError(
                StatusCode::GONE,
                "Session is closing or closed.".into(),
            ));
        }
        if let Some(error) = &run.fault {
            return Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, error.clone()));
        }
        if revision < run.revision {
            if let Some(accepted) = &run.last_command
                && accepted.revision == revision
                && accepted.fingerprint == fingerprint
            {
                return Ok(json_response(accepted.reply.clone()));
            }
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Revision is stale or was used by a different command.".into(),
            ));
        }
        if revision != run.revision {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Revision is ahead of the server.".into(),
            ));
        }
        run.execute(command, events_only, Some(fingerprint)).await
    })
    .await
    .map_err(background_task_error)?
}

async fn step(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Json(input): Json<Step>,
) -> ApiResult {
    let (command, millis) = match (input.seconds, input.milliseconds) {
        (Some(seconds), None) => (Command::Advance(seconds), seconds.saturating_mul(1000)),
        (None, Some(milliseconds)) => (Command::AdvanceMillis(milliseconds), milliseconds),
        _ => {
            return Err(ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Specify seconds or milliseconds.".into(),
            ));
        }
    };
    mutate(
        &app,
        id,
        input.revision,
        command,
        input.events_only,
        format!("step:{millis}:events_only={}", input.events_only),
    )
    .await
}

async fn place(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Json(input): Json<Place>,
) -> ApiResult {
    mutate(
        &app,
        id,
        input.revision,
        Command::Place {
            lots: input.lots,
            spot_limit_cents: input.spot_limit_cents,
            future_limit_cents: input.future_limit_cents,
        },
        false,
        format!(
            "place:{}:{}:{}",
            input.lots, input.spot_limit_cents, input.future_limit_cents
        ),
    )
    .await
}

async fn enter(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Json(input): Json<Enter>,
) -> ApiResult {
    let Place {
        revision,
        lots,
        spot_limit_cents,
        future_limit_cents,
    } = input.pair;
    let cross_market = input.cross_market;
    mutate(
        &app,
        id,
        revision,
        Command::Enter {
            lots,
            spot_limit_cents,
            future_limit_cents,
            cross_market,
        },
        false,
        format!("entry:{cross_market:?}:{lots}:{spot_limit_cents}:{future_limit_cents}"),
    )
    .await
}

async fn cancel(
    State(app): State<Arc<App>>,
    Path((id, order)): Path<(u64, u64)>,
    Json(input): Json<Revision>,
) -> ApiResult {
    mutate(
        &app,
        id,
        input.revision,
        Command::Cancel(order),
        false,
        format!("cancel:{order}"),
    )
    .await
}

async fn cross(
    State(app): State<Arc<App>>,
    Path((id, order)): Path<(u64, u64)>,
    Json(input): Json<Revision>,
) -> ApiResult {
    mutate(
        &app,
        id,
        input.revision,
        Command::Cross(order),
        false,
        format!("cross:{order}"),
    )
    .await
}

async fn hedge(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Json(input): Json<Revision>,
) -> ApiResult {
    mutate(
        &app,
        id,
        input.revision,
        Command::Hedge,
        false,
        "hedge".into(),
    )
    .await
}

#[derive(Deserialize)]
struct ReplayStart {
    transport: Transport,
}

#[derive(Deserialize)]
struct ReplayBatch {
    revision: u64,
    events: Vec<Event>,
}

async fn replay_start(State(app): State<Arc<App>>, Json(input): Json<ReplayStart>) -> ApiResult {
    tokio::spawn(start_replay(app, input))
        .await
        .map_err(background_task_error)?
}

async fn start_replay(app: Arc<App>, input: ReplayStart) -> ApiResult {
    let _lifecycle = app.lifecycle.lock().await;
    let id = app.allocate_run_id()?;
    let kafka = create_transport(&app, input.transport, id).await?;
    let at_capacity = app.replays.lock().await.len() >= MAX_REPLAYS;
    if at_capacity && let Err(error) = evict_oldest_replay(&app).await {
        return Err(cleanup_after_failed_start(
            kafka.as_ref(),
            ApiError(StatusCode::SERVICE_UNAVAILABLE, error),
        )
        .await);
    }
    let run = ReplayRun {
        kafka,
        revision: 0,
        clock_ms: 0,
        last_reply: json!(null),
        fault: None,
        last_events: None,
        closed: false,
    };
    let mut replays = app.replays.lock().await;
    replays.insert(id, Arc::new(Mutex::new(run)));
    Ok(json_response(json!({"id": id, "revision": 0})))
}

async fn replay_batch(
    State(app): State<Arc<App>>,
    Path(id): Path<u64>,
    Json(input): Json<ReplayBatch>,
) -> ApiResult {
    if input.events.is_empty() || input.events.len() > 1000 {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Replay batches require 1–1000 events.".into(),
        ));
    }
    let run = app
        .replays
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Replay expired.".into()))?;
    tokio::spawn(async move {
        let mut run = run.lock().await;
        if run.closed {
            return Err(ApiError(
                StatusCode::GONE,
                "Replay is closing or closed.".into(),
            ));
        }
        if let Some(fault) = &run.fault {
            return Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, fault.clone()));
        }
        if run.revision > 0 && input.revision == run.revision - 1 {
            if run.last_events.as_ref() == Some(&input.events) {
                return Ok(json_response(run.last_reply.clone()));
            }
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Replay retry payload differs from the accepted batch.".into(),
            ));
        }
        if input.revision != run.revision {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Replay revision mismatch.".into(),
            ));
        }
        let mut clock = run.clock_ms;
        for event in &input.events {
            if event.clock_ms < clock || event.clock_ms > 86_400_000 {
                return Err(ApiError(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Replay timestamps must be ordered within a day.".into(),
                ));
            }
            clock = event.clock_ms;
        }
        let started = std::time::Instant::now();
        let accepted_events = input.events.clone();
        let delivered = match &run.kafka {
            Some(kafka) => {
                tokio::time::timeout(KAFKA_TIMEOUT, kafka.deliver_measured(input.events))
                    .await
                    .map_err(|_| "Kafka replay timed out.".to_string())
                    .and_then(|result| result)
            }
            None => Ok(transport::Delivery {
                events: input.events,
                timings: Default::default(),
            }),
        };
        let delivered = match delivered {
            Ok(delivered) => delivered,
            Err(error) => {
                run.fault = Some(error.clone());
                return Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, error));
            }
        };
        run.clock_ms = clock;
        run.revision += 1;
        run.last_events = Some(accepted_events);
        let mut reply = serde_json::to_value(delivered)
            .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        reply["revision"] = json!(run.revision);
        reply["server_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
        run.last_reply = reply.clone();
        Ok(json_response(reply))
    })
    .await
    .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
}

fn web_file(path: &str) -> Response {
    const HTML: &str = "text/html; charset=utf-8";
    const CSS: &str = "text/css; charset=utf-8";
    const JS: &str = "text/javascript; charset=utf-8";
    let (mime, body) = match path {
        "showcase.html" => (HTML, include_str!("../web/showcase.html")),
        "showcase.css" => (CSS, include_str!("../web/showcase.css")),
        "showcase.mjs" => (JS, include_str!("../web/showcase.mjs")),
        "market-charts.mjs" => (JS, include_str!("../web/market-charts.mjs")),
        "live-showcase.mjs" => (JS, include_str!("../web/live-showcase.mjs")),
        "browser-artifact.mjs" => (JS, include_str!("../web/browser-artifact.mjs")),
        "entry-policy.mjs" => (JS, include_str!("../web/entry-policy.mjs")),
        "hedge-policy.mjs" => (JS, include_str!("../web/hedge-policy.mjs")),
        "hedge-comparison.mjs" => (JS, include_str!("../web/hedge-comparison.mjs")),
        "finance.mjs" => (JS, include_str!("../web/finance.mjs")),
        "event-activity.mjs" => (JS, include_str!("../web/event-activity.mjs")),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    (
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

async fn showcase() -> Response {
    web_file("showcase.html")
}

async fn asset(Path(path): Path<String>) -> Response {
    web_file(&path)
}

fn allowed_bind(bind: SocketAddr, container: bool) -> bool {
    bind.ip().is_loopback() || (container && bind.ip().is_unspecified())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|argument| argument == "--verify-day")
    {
        let mut kafka = false;
        let mut duration_ms = 86_400_000;
        let mut index = 1;
        while index < arguments.len() {
            match arguments[index].as_str() {
                "--local" => kafka = false,
                "--kafka" => kafka = true,
                "--duration-seconds" => {
                    index += 1;
                    let seconds: u64 = arguments
                        .get(index)
                        .ok_or("Missing verification duration")?
                        .parse()?;
                    if !(5..=86_400).contains(&seconds) {
                        return Err(
                            "Verification duration must be between 5 and 86400 seconds".into()
                        );
                    }
                    duration_ms = seconds * 1000;
                }
                argument => return Err(format!("Unknown verification option: {argument}").into()),
            }
            index += 1;
        }
        let broker = if kafka {
            Some(
                std::env::var("BASIS_KAFKA_BROKER")
                    .map_err(|_| "Set BASIS_KAFKA_BROKER for Kafka verification")?,
            )
        } else {
            None
        };
        let stdout = std::io::stdout();
        let mut output = std::io::BufWriter::with_capacity(262_144, stdout.lock());
        verify::run(&mut output, broker.as_deref(), duration_ms)
            .await
            .map_err(std::io::Error::other)?;
        std::io::Write::flush(&mut output)?;
        return Ok(());
    }
    if !arguments.is_empty() {
        return Err("Unknown arguments; use --verify-day [--local|--kafka]".into());
    }
    let bind: SocketAddr = std::env::var("BASIS_BIND")
        .unwrap_or_else(|_| "127.0.0.1:4330".into())
        .parse()?;
    let container_bind = std::env::var("BASIS_ALLOW_CONTAINER_BIND").as_deref() == Ok("1");
    if !allowed_bind(bind, container_bind) {
        return Err(
            "BASIS_BIND must be loopback, unless explicitly configured for a container bridge."
                .into(),
        );
    }
    let app = Arc::new(App {
        broker: std::env::var("BASIS_KAFKA_BROKER")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        // Keep IDs numeric and sortable within a process without reusing the old server's IDs.
        next_id: AtomicU64::new(rand::random::<u64>() & MAX_SAFE_INTEGER & !RUN_COUNTER_MASK),
        lifecycle: Mutex::new(()),
        sessions: Mutex::new(BTreeMap::new()),
        replays: Mutex::new(BTreeMap::new()),
    });
    let router = Router::new()
        .route("/", get(showcase))
        .route("/demo", get(showcase))
        .route("/api/config", get(config))
        .route("/api/sessions", post(start))
        .route("/api/sessions/{id}", get(current).delete(close_session))
        .route("/api/sessions/{id}/step", post(step))
        .route("/api/sessions/{id}/orders", post(place))
        .route("/api/sessions/{id}/entry", post(enter))
        .route("/api/sessions/{id}/hedge", post(hedge))
        .route("/api/sessions/{id}/orders/{order}/cancel", post(cancel))
        .route("/api/sessions/{id}/orders/{order}/cross", post(cross))
        .route("/api/replays", post(replay_start))
        .route("/api/replays/{id}/batch", post(replay_batch))
        .route("/api/replays/{id}", delete(close_replay))
        .route("/{*path}", get(asset))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("BTC Market Feed at http://{}", listener.local_addr()?);
    let nodelay = std::env::var("BASIS_HTTP_NODELAY").as_deref() != Ok("false");
    axum::serve(
        listener.tap_io(move |stream| {
            if let Err(error) = stream.set_nodelay(nodelay) {
                eprintln!("TCP_NODELAY: {error}");
            }
        }),
        router,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::market::Message;
    use super::*;

    #[test]
    fn public_bind_requires_the_explicit_container_wildcard() {
        assert!(allowed_bind("127.0.0.1:4330".parse().unwrap(), false));
        assert!(allowed_bind("[::1]:4330".parse().unwrap(), false));
        assert!(!allowed_bind("0.0.0.0:4330".parse().unwrap(), false));
        assert!(allowed_bind("0.0.0.0:4330".parse().unwrap(), true));
        assert!(!allowed_bind("192.0.2.1:4330".parse().unwrap(), true));
    }

    fn app_with_session(id: u64, run: Run) -> Arc<App> {
        Arc::new(App {
            broker: None,
            next_id: AtomicU64::new(id + 1),
            lifecycle: Mutex::new(()),
            sessions: Mutex::new(BTreeMap::from([(id, Arc::new(Mutex::new(run)))])),
            replays: Mutex::new(BTreeMap::new()),
        })
    }

    fn run(id: u64) -> Run {
        Run {
            id,
            revision: 0,
            sim: Session::new(73),
            kafka: None,
            fault: None,
            recent: VecDeque::new(),
            clock_ms: 0,
            previous: None,
            last_command: None,
            closed: false,
        }
    }

    fn status(result: ApiResult) -> StatusCode {
        match result {
            Ok(response) => response.status(),
            Err(error) => error.0,
        }
    }

    #[tokio::test]
    async fn only_exact_latest_command_retry_uses_cached_reply() {
        let mut run = run(7);
        run.revision = 3;
        run.last_command = Some(AcceptedCommand {
            revision: 2,
            fingerprint: "step:100:events_only=true".into(),
            reply: json!({"revision": 3, "marker": "cached"}),
        });
        let app = app_with_session(7, run);

        let exact = mutate(
            &app,
            7,
            2,
            Command::AdvanceMillis(100),
            true,
            "step:100:events_only=true".into(),
        )
        .await;
        assert_eq!(status(exact), StatusCode::OK);
        let changed = mutate(
            &app,
            7,
            2,
            Command::AdvanceMillis(101),
            true,
            "step:101:events_only=true".into(),
        )
        .await;
        assert_eq!(status(changed), StatusCode::CONFLICT);
        let older = mutate(
            &app,
            7,
            1,
            Command::AdvanceMillis(100),
            true,
            "step:100:events_only=true".into(),
        )
        .await;
        assert_eq!(status(older), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn failed_kafka_start_preserves_runs_at_capacity() {
        let app = app_with_session(1, run(1));
        for _ in 1..MAX_SESSIONS {
            let candidate = Start {
                seed: 73,
                transport: Transport::Local,
                scenario: Scenario::Reference,
            };
            assert_eq!(
                status(start_session(app.clone(), candidate).await),
                StatusCode::OK
            );
        }
        for _ in 0..MAX_REPLAYS {
            let candidate = ReplayStart {
                transport: Transport::Local,
            };
            assert_eq!(
                status(start_replay(app.clone(), candidate).await),
                StatusCode::OK
            );
        }
        let oldest_replay = *app.replays.lock().await.first_key_value().unwrap().0;

        let session = Start {
            seed: 73,
            transport: Transport::Kafka,
            scenario: Scenario::Reference,
        };
        assert_eq!(
            status(start_session(app.clone(), session).await),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let replay = ReplayStart {
            transport: Transport::Kafka,
        };
        assert_eq!(
            status(start_replay(app.clone(), replay).await),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(app.sessions.lock().await.contains_key(&1));
        assert!(app.replays.lock().await.contains_key(&oldest_replay));

        let session = Start {
            seed: 73,
            transport: Transport::Local,
            scenario: Scenario::Reference,
        };
        assert_eq!(
            status(start_session(app.clone(), session).await),
            StatusCode::OK
        );
        assert_eq!(app.sessions.lock().await.len(), MAX_SESSIONS);
        assert!(!app.sessions.lock().await.contains_key(&1));
    }

    #[tokio::test]
    async fn closed_existing_session_and_replay_handles_reject_queued_work() {
        let mut session = run(8);
        session.closed = true;
        let app = app_with_session(8, session);
        let result = mutate(
            &app,
            8,
            0,
            Command::AdvanceMillis(1),
            false,
            "step:1:events_only=false".into(),
        )
        .await;
        assert_eq!(status(result), StatusCode::GONE);

        app.replays.lock().await.insert(
            9,
            Arc::new(Mutex::new(ReplayRun {
                kafka: None,
                revision: 0,
                clock_ms: 0,
                last_reply: json!(null),
                fault: None,
                last_events: None,
                closed: true,
            })),
        );
        let event = Event {
            clock_ms: 0,
            message: Message::Tick {
                rng: 1,
                fair_cents: 1,
                basis_cents: 0,
                sample: false,
                elapsed_ms: 0,
                next_event_ms: 1,
                market_path: None,
            },
        };
        let result = replay_batch(
            State(app.clone()),
            Path(9),
            Json(ReplayBatch {
                revision: 0,
                events: vec![event],
            }),
        )
        .await;
        assert_eq!(status(result), StatusCode::GONE);
    }

    #[tokio::test]
    async fn close_tombstones_shared_handle_before_waiting_mutation_can_commit() {
        let app = app_with_session(10, run(10));
        let shared = app.sessions.lock().await.get(&10).unwrap().clone();
        let held = shared.lock().await;
        let closing_app = app.clone();
        let close = tokio::spawn(async move { close_session(State(closing_app), Path(10)).await });
        while let Ok(guard) = app.lifecycle.try_lock() {
            drop(guard);
            tokio::task::yield_now().await;
        }

        let mutating_app = app.clone();
        let mutation = tokio::spawn(async move {
            mutate(
                &mutating_app,
                10,
                0,
                Command::AdvanceMillis(1),
                false,
                "step:1:events_only=false".into(),
            )
            .await
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        drop(held);

        let close_status = status(close.await.expect("close task"));
        let mutation_status = status(mutation.await.expect("mutation task"));
        assert_eq!(close_status, StatusCode::OK);
        assert!(matches!(
            mutation_status,
            StatusCode::GONE | StatusCode::NOT_FOUND
        ));
        assert!(!app.sessions.lock().await.contains_key(&10));
    }
}
