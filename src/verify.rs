//! Bounded-memory full-day Rust/Kafka/browser verification stream.

use std::io::Write;
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};

use crate::market::{Command, Event, MarketId, Session};
use crate::transport::{DeliveryTimings, KafkaRun};

const DAY_MS: u64 = 86_400_000;
const HOUR_MS: u64 = 3_600_000;
const BATCH_MS: u32 = 300_000;
const KAFKA_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize)]
struct EventRow<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    event: &'a Event,
}

fn line(output: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    serde_json::to_writer(&mut *output, value).map_err(|error| error.to_string())?;
    output.write_all(b"\n").map_err(|error| error.to_string())
}

fn accumulate(total: &mut DeliveryTimings, next: &DeliveryTimings) {
    total.encode_ms += next.encode_ms;
    total.produce_ms += next.produce_ms;
    total.fetch_ms += next.fetch_ms;
    total.decode_validate_ms += next.decode_validate_ms;
    total.total_ms += next.total_ms;
    total.records += next.records;
    total.produce_requests += next.produce_requests;
    total.fetch_requests += next.fetch_requests;
}

async fn commit<W: Write>(
    session: &mut Session,
    kafka: Option<&KafkaRun>,
    output: &mut W,
    command: Command,
    event_count: &mut u64,
    timings: &mut DeliveryTimings,
) -> Result<(), String> {
    let planned = session.plan(command)?;
    let events = if let Some(kafka) = kafka {
        let delivered = tokio::time::timeout(KAFKA_TIMEOUT, kafka.deliver_measured(planned))
            .await
            .map_err(|_| "Kafka verification delivery timed out".to_owned())??;
        accumulate(timings, &delivered.timings);
        delivered.events
    } else {
        planned
    };
    for event in &events {
        line(output, &EventRow { kind: "event", event })?;
    }
    *event_count += events.len() as u64;
    session.apply(events)
}

/// Runs the canonical seeded path. `broker=None` runs local delivery; a broker
/// performs exact Kafka produce/readback before events enter the Rust projection.
pub async fn run<W: Write>(
    output: &mut W,
    broker: Option<&str>,
    duration_ms: u64,
) -> Result<(), String> {
    if !(5_000..=DAY_MS).contains(&duration_ms) {
        return Err("Verification duration must be 5000..=86400000 ms".into());
    }
    let kafka = if let Some(broker) = broker {
        Some(KafkaRun::new(broker, 73).await.map_err(|error| format!("Kafka setup: {error}"))?)
    } else {
        None
    };
    let topics = kafka.as_ref().map(|run| json!({"market":run.market_topic,"private":run.private_topic}));
    let result = run_inner(output, kafka.as_ref(), duration_ms).await;
    let cleanup = if let Some(run) = &kafka { run.cleanup_topics().await } else { Ok(()) };
    match (result, cleanup) {
        (Ok((final_snapshot, event_count, initialization_event_count, timings)), Ok(())) => {
            line(output, &json!({
                "type":"summary",
                "transport":if kafka.is_some(){"kafka"}else{"local"},
                "topics":topics,
                "duration_ms":duration_ms,
                "complete_day":duration_ms == DAY_MS,
                "event_count":event_count,
                "initialization_event_count":initialization_event_count,
                "kafka_timings":timings,
                "final_snapshot":final_snapshot
            }))
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!("Kafka topic cleanup: {error}")),
        (Err(error), Err(cleanup)) => Err(format!("{error}; Kafka topic cleanup: {cleanup}")),
    }
}

async fn run_inner<W: Write>(
    output: &mut W,
    kafka: Option<&KafkaRun>,
    duration_ms: u64,
) -> Result<(Value, u64, u64, DeliveryTimings), String> {
    let mut session = Session::new(73);
    let mut event_count = 0u64;
    let mut timings = DeliveryTimings::default();

    // Initialization also uses Kafka readback when configured. The browser starts
    // at this canonical baseline and independently projects every subsequent event.
    let initial = session.plan(Command::Initialize)?;
    let initial = if let Some(kafka) = kafka {
        let delivered = tokio::time::timeout(KAFKA_TIMEOUT, kafka.deliver_measured(initial))
            .await
            .map_err(|_| "Kafka initialization timed out".to_owned())??;
        accumulate(&mut timings, &delivered.timings);
        delivered.events
    } else {
        initial
    };
    let initialization_event_count = initial.len() as u64;
    event_count += initialization_event_count;
    session.apply(initial)?;
    line(output, &json!({"type":"initial", "snapshot":session.snapshot()}))?;

    // Deterministic own-order lifecycle: passive paired orders, cancellations,
    // a spot-only cross and an explicit residual checkpoint, then the hedge cross.
    commit(&mut session, kafka, output, Command::AdvanceMillis(1_000), &mut event_count, &mut timings).await?;
    let snapshot = session.snapshot();
    let spot_bid = snapshot["books"][0]["bid_cents"].as_i64().ok_or("Missing spot bid")?;
    let future_ask = snapshot["books"][1]["ask_cents"].as_i64().ok_or("Missing future ask")?;
    commit(&mut session, kafka, output, Command::Place {
        lots: 100, spot_limit_cents: spot_bid, future_limit_cents: future_ask,
    }, &mut event_count, &mut timings).await?;
    let first_ids: Vec<u64> = session.snapshot()["orders"].as_array().ok_or("Missing orders")?
        .iter().map(|order| order["id"].as_str().and_then(|id| id.parse().ok()).ok_or("Invalid own order ID"))
        .collect::<Result<_, _>>()?;
    for id in first_ids {
        commit(&mut session, kafka, output, Command::Cancel(id), &mut event_count, &mut timings).await?;
    }
    let snapshot = session.snapshot();
    let spot_bid = snapshot["books"][0]["bid_cents"].as_i64().ok_or("Missing spot bid")?;
    let future_ask = snapshot["books"][1]["ask_cents"].as_i64().ok_or("Missing future ask")?;
    commit(&mut session, kafka, output, Command::Place {
        lots: 100, spot_limit_cents: spot_bid, future_limit_cents: future_ask,
    }, &mut event_count, &mut timings).await?;
    let active_ids: Vec<(u64, MarketId)> = session.snapshot()["orders"].as_array().ok_or("Missing orders")?
        .iter().filter(|order| matches!(order["status"].as_str(), Some("working" | "partial")))
        .map(|order| {
            let id = order["id"].as_str().and_then(|id| id.parse().ok()).ok_or("Invalid own order ID")?;
            let market = match order["market"].as_str() {
                Some("spot") => MarketId::Spot,
                Some("future") => MarketId::Future,
                _ => return Err("Invalid own order market"),
            };
            Ok((id, market))
        }).collect::<Result<_, &str>>()?;
    let spot_id = active_ids.iter().find(|(_, market)| *market == MarketId::Spot)
        .map(|entry| entry.0).ok_or("Missing working spot order")?;
    let future_id = active_ids.iter().find(|(_, market)| *market == MarketId::Future)
        .map(|entry| entry.0).ok_or("Missing working future order")?;
    commit(&mut session, kafka, output, Command::Cross(spot_id), &mut event_count, &mut timings).await?;
    let residual = session.snapshot();
    if residual["metrics"]["residual_btc"].as_f64() != Some(0.1) {
        return Err("Spot leg did not produce the expected positive 0.1 BTC residual".into());
    }
    line(output, &json!({"type":"checkpoint", "checkpoint_kind":"residual", "clock_ms":residual["clock_ms"], "snapshot":residual}))?;
    commit(&mut session, kafka, output, Command::Cross(future_id), &mut event_count, &mut timings).await?;

    let mut elapsed_ms = session.snapshot()["elapsed_ms"].as_u64().ok_or("Missing elapsed clock")?;
    let mut target = HOUR_MS.min(duration_ms);
    while elapsed_ms < duration_ms {
        let step = (target - elapsed_ms).min(u64::from(BATCH_MS));
        commit(&mut session, kafka, output, Command::AdvanceMillis(step as u32), &mut event_count, &mut timings).await?;
        elapsed_ms += step;
        if elapsed_ms == target {
            line(output, &json!({"type":"checkpoint", "checkpoint_kind":if target == duration_ms {"final"} else {"hourly"}, "clock_ms":target, "snapshot":session.snapshot()}))?;
            target = (target + HOUR_MS).min(duration_ms);
        }
    }
    let final_snapshot = session.snapshot();
    if duration_ms == DAY_MS && final_snapshot["clock_ms"].as_u64() != Some(DAY_MS) {
        return Err("Full-day run did not reach the canonical close".into());
    }
    Ok((final_snapshot, event_count, initialization_event_count, timings))
}
