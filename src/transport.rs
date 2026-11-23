use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rskafka::{
    client::{
        ClientBuilder,
        controller::ControllerClient,
        error::{Error as KafkaError, ProtocolError},
        partition::{Compression, PartitionClient, UnknownTopicHandling},
    },
    record::Record,
};
use tokio::sync::Mutex;

static TOPIC_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DeliveryTimings {
    pub encode_ms: f64,
    /// Sum of sequential produce request wall times.
    pub produce_ms: f64,
    /// Sum of sequential fetch request wall times.
    pub fetch_ms: f64,
    /// Local header, JSON, topic, and exact-payload validation time.
    pub decode_validate_ms: f64,
    pub total_ms: f64,
    pub records: usize,
    pub produce_requests: usize,
    pub fetch_requests: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct Delivery {
    pub events: Vec<crate::market::Event>,
    pub timings: DeliveryTimings,
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn ignore_already_deleted_topic(result: Result<(), KafkaError>) -> Result<(), KafkaError> {
    match result {
        Ok(())
        | Err(KafkaError::ServerError {
            protocol_error: ProtocolError::UnknownTopicOrPartition,
            ..
        }) => Ok(()),
        Err(error) => Err(error),
    }
}

pub struct KafkaRun {
    controller: ControllerClient,
    market_partition: PartitionClient,
    private_partition: PartitionClient,
    next_sequence: AtomicU64,
    delivery_lock: Mutex<()>,
    market_deleted: AtomicBool,
    private_deleted: AtomicBool,
    pub market_topic: String,
    pub private_topic: String,
}

impl KafkaRun {
    pub async fn new(broker: &str, run_id: u64) -> Result<Self, String> {
        let client = ClientBuilder::new(vec![broker.to_owned()])
            .build()
            .await
            .map_err(|e| format!("Kafka connect: {e}"))?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("System clock: {e}"))?
            .as_nanos();
        let nonce = TOPIC_NONCE.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("btc-run-{run_id}-{stamp:x}-{nonce:x}");
        let market_topic = format!("{prefix}-market-l3");
        let private_topic = format!("{prefix}-private-execution");
        let admin = client
            .controller_client()
            .map_err(|e| format!("Kafka controller: {e}"))?;
        if let Err(error) = admin.create_topic(&market_topic, 1, 1, 5_000).await {
            let _ = admin.delete_topic(&market_topic, 5_000).await;
            return Err(format!("Create topic {market_topic}: {error}"));
        }
        if let Err(error) = admin
            .create_topic(&private_topic, 1, 1, 5_000)
            .await
        {
            let _ = admin.delete_topic(&market_topic, 5_000).await;
            let _ = admin.delete_topic(&private_topic, 5_000).await;
            return Err(format!("Create topic {private_topic}: {error}"));
        }
        let market_partition = match client
            .partition_client(market_topic.clone(), 0, UnknownTopicHandling::Retry)
            .await
        {
            Ok(partition) => partition,
            Err(error) => {
                let _ = admin.delete_topic(&market_topic, 5_000).await;
                let _ = admin.delete_topic(&private_topic, 5_000).await;
                return Err(format!("Open partition {market_topic}: {error}"));
            }
        };
        let private_partition = match client
            .partition_client(private_topic.clone(), 0, UnknownTopicHandling::Retry)
            .await
        {
            Ok(partition) => partition,
            Err(error) => {
                let _ = admin.delete_topic(&market_topic, 5_000).await;
                let _ = admin.delete_topic(&private_topic, 5_000).await;
                return Err(format!("Open partition {private_topic}: {error}"));
            }
        };
        Ok(Self {
            controller: admin,
            market_partition,
            private_partition,
            next_sequence: AtomicU64::new(0),
            delivery_lock: Mutex::new(()),
            market_deleted: AtomicBool::new(false),
            private_deleted: AtomicBool::new(false),
            market_topic,
            private_topic,
        })
    }

    pub async fn cleanup_topics(&self) -> Result<(), String> {
        if !self.market_deleted.load(Ordering::Acquire) {
            ignore_already_deleted_topic(self.controller.delete_topic(&self.market_topic, 5_000).await)
                .map_err(|error| format!("Delete topic {}: {error}", self.market_topic))?;
            self.market_deleted.store(true, Ordering::Release);
        }
        if !self.private_deleted.load(Ordering::Acquire) {
            ignore_already_deleted_topic(self.controller.delete_topic(&self.private_topic, 5_000).await)
                .map_err(|error| format!("Delete topic {}: {error}", self.private_topic))?;
            self.private_deleted.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub async fn deliver_measured(
        &self,
        events: Vec<crate::market::Event>,
    ) -> Result<Delivery, String> {
        let total_started = Instant::now();
        let _guard = self.delivery_lock.lock().await;
        let mut timings = DeliveryTimings {
            records: events.len(),
            ..Default::default()
        };
        let encode_started = Instant::now();
        let mut market = Vec::new();
        let mut private = Vec::new();
        let mut order = Vec::with_capacity(events.len());
        for event in events {
            let sequence = self
                .next_sequence
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| "Kafka event sequence exhausted".to_owned())?;
            let bytes = serde_json::to_vec(&event)
                .map_err(|e| format!("Serialize event {sequence}: {e}"))?;
            let mut headers = BTreeMap::new();
            headers.insert("sequence".to_owned(), sequence.to_be_bytes().to_vec());
            let record = Record {
                key: None,
                value: Some(bytes),
                headers,
                timestamp: chrono::DateTime::<chrono::Utc>::from(SystemTime::now()),
            };
            if event.is_private() {
                private.push((sequence, event, record));
            } else {
                market.push((sequence, event, record));
            }
            order.push(sequence);
        }
        timings.encode_ms = millis(encode_started.elapsed());

        let mut readbacks = BTreeMap::new();
        readbacks.extend(
            publish_and_read(
                &self.market_partition,
                &self.market_topic,
                false,
                market,
                &mut timings,
            )
            .await?,
        );
        for (sequence, event) in publish_and_read(
            &self.private_partition,
            &self.private_topic,
            true,
            private,
            &mut timings,
        )
        .await?
        {
            if readbacks.insert(sequence, event).is_some() {
                return Err(format!("Duplicate Kafka sequence {sequence} across topics"));
            }
        }
        let events = order
            .into_iter()
            .map(|sequence| {
                readbacks
                    .remove(&sequence)
                    .ok_or_else(|| format!("Kafka readback missing sequence {sequence}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        timings.total_ms = millis(total_started.elapsed());
        Ok(Delivery { events, timings })
    }
}

async fn publish_and_read(
    partition: &PartitionClient,
    topic: &str,
    expect_private: bool,
    batch: Vec<(u64, crate::market::Event, Record)>,
    timings: &mut DeliveryTimings,
) -> Result<Vec<(u64, crate::market::Event)>, String> {
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let mut all_readbacks = Vec::with_capacity(batch.len());
    for chunk in batch.chunks(200) {
        all_readbacks.extend(
            publish_chunk_and_read(partition, topic, expect_private, chunk, timings).await?,
        );
    }
    Ok(all_readbacks)
}

async fn publish_chunk_and_read(
    partition: &PartitionClient,
    topic: &str,
    expect_private: bool,
    batch: &[(u64, crate::market::Event, Record)],
    timings: &mut DeliveryTimings,
) -> Result<Vec<(u64, crate::market::Event)>, String> {
    let expected: BTreeMap<u64, crate::market::Event> = batch
        .iter()
        .map(|(sequence, event, _)| (*sequence, event.clone()))
        .collect();
    let sequences: Vec<u64> = batch.iter().map(|(sequence, _, _)| *sequence).collect();
    let records: Vec<Record> = batch.iter().map(|(_, _, record)| record.clone()).collect();
    let produce_started = Instant::now();
    let offsets = partition
        .produce(records, Compression::NoCompression)
        .await
        .map_err(|e| format!("Publish {} events to {topic}: {e}", sequences.len()))?;
    timings.produce_ms += millis(produce_started.elapsed());
    timings.produce_requests += 1;
    if offsets.len() != sequences.len() {
        return Err(format!(
            "Kafka returned {} offsets for {} events on {topic}",
            offsets.len(),
            sequences.len()
        ));
    }
    let mut pending: BTreeMap<i64, u64> = offsets
        .into_iter()
        .zip(sequences.iter().copied())
        .collect();
    if pending.len() != sequences.len() {
        return Err(format!("Kafka returned duplicate offsets on {topic}"));
    }
    let mut readbacks = BTreeMap::new();
    let mut last_progress = tokio::time::Instant::now();
    while !pending.is_empty() {
        let offset = *pending.keys().next().expect("pending is not empty");
        let fetch_started = Instant::now();
        let (records, _) = partition
            .fetch_records(offset, 1..1_048_576, 1_000)
            .await
            .map_err(|e| format!("Read back events from {topic} at {offset}: {e}"))?;
        timings.fetch_ms += millis(fetch_started.elapsed());
        timings.fetch_requests += 1;
        let validate_started = Instant::now();
        let mut progressed = false;
        for item in records {
            let sequence = pending.remove(&item.offset).ok_or_else(|| {
                format!(
                    "Unexpected or duplicate Kafka offset {} on {topic}",
                    item.offset
                )
            })?;
            let header = item
                .record
                .headers
                .get("sequence")
                .and_then(|value| <[u8; 8]>::try_from(value.as_slice()).ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| {
                    format!(
                        "Missing or invalid sequence header at {topic}:{}",
                        item.offset
                    )
                })?;
            if header != sequence {
                return Err(format!(
                    "Sequence mismatch at {topic}:{}: expected {sequence}, got {header}",
                    item.offset
                ));
            }
            let payload = item
                .record
                .value
                .as_deref()
                .ok_or_else(|| format!("Empty Kafka value at {topic}:{}", item.offset))?;
            let event: crate::market::Event = serde_json::from_slice(payload)
                .map_err(|e| format!("Deserialize event {sequence} from {topic}: {e}"))?;
            if event.is_private() != expect_private {
                return Err(format!(
                    "Event {sequence} was read from the wrong topic {topic}"
                ));
            }
            if expected.get(&sequence) != Some(&event) {
                return Err(format!(
                    "Kafka payload mismatch for event {sequence} on {topic}"
                ));
            }
            if readbacks.insert(sequence, event).is_some() {
                return Err(format!("Duplicate Kafka sequence {sequence} on {topic}"));
            }
            progressed = true;
        }
        if progressed {
            last_progress = tokio::time::Instant::now();
        } else if last_progress.elapsed() >= std::time::Duration::from_secs(12) {
            return Err(format!(
                "Timed out waiting for {} Kafka offsets on {topic}",
                pending.len()
            ));
        }
        timings.decode_validate_ms += millis(validate_started.elapsed());
    }
    if readbacks.len() != sequences.len() {
        return Err(format!("Readback count mismatch on {topic}"));
    }
    Ok(readbacks.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rskafka::client::error::RequestContext;

    fn server_error(protocol_error: ProtocolError) -> KafkaError {
        KafkaError::ServerError {
            protocol_error,
            error_message: None,
            request: RequestContext::Topic("btc-run-test-market".into()),
            response: None,
            is_virtual: false,
        }
    }

    #[test]
    fn topic_cleanup_treats_unknown_topic_as_already_deleted() {
        assert!(ignore_already_deleted_topic(Err(server_error(
            ProtocolError::UnknownTopicOrPartition,
        )))
        .is_ok());
    }

    #[test]
    fn topic_cleanup_preserves_other_broker_errors() {
        assert!(matches!(
            ignore_already_deleted_topic(Err(server_error(ProtocolError::RequestTimedOut))),
            Err(KafkaError::ServerError {
                protocol_error: ProtocolError::RequestTimedOut,
                ..
            })
        ));
    }
}
