use std::{fmt, ops::Range, sync::Arc, time::Duration};

use protocol::{
    engine::{EngineEvent, EngineInput, EngineReply},
    wallet::WalletEvent,
};
use rskafka::{
    client::{
        Client, ClientBuilder,
        error::Error as ClientError,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    topic::Topic,
};
use tokio::task::JoinSet;

use crate::{settings::ProjectorSettings, worker::ProjectorWorker};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);
const MISSING_CONTEXT_SLEEP: Duration = Duration::from_millis(500);

pub struct ProjectorQueue {
    sources: Vec<ProjectorSource>,
}

impl ProjectorQueue {
    pub async fn new(settings: &ProjectorSettings) -> Result<Self, QueueError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-projector")
            .build()
            .await?;
        let topics = client.list_topics().await?;

        let sources = vec![
            ProjectorSource::new(
                SourceKind::EngineInput,
                settings.engine_input_topic.clone(),
                partition_clients(&client, &topics, &settings.engine_input_topic).await?,
            ),
            ProjectorSource::new(
                SourceKind::EngineReply,
                settings.engine_replies_topic.clone(),
                partition_clients(&client, &topics, &settings.engine_replies_topic).await?,
            ),
            ProjectorSource::new(
                SourceKind::EngineEvent,
                settings.engine_events_topic.clone(),
                partition_clients(&client, &topics, &settings.engine_events_topic).await?,
            ),
            ProjectorSource::new(
                SourceKind::WalletEvent,
                settings.wallet_events_topic.clone(),
                partition_clients(&client, &topics, &settings.wallet_events_topic).await?,
            ),
        ];

        Ok(Self { sources })
    }

    pub async fn run(self, worker: ProjectorWorker) -> Result<(), QueueError> {
        let mut tasks = JoinSet::new();

        for source in self.sources {
            for partition_client in source.partitions {
                let topic = source.topic.clone();
                let worker = worker.clone();
                let kind = source.kind;

                tasks.spawn(async move {
                    run_source_partition(kind, topic, partition_client, worker).await
                });
            }
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(QueueError::Task(error.to_string())),
            }
        }

        Ok(())
    }
}

struct ProjectorSource {
    kind: SourceKind,
    topic: String,
    partitions: Vec<Arc<PartitionClient>>,
}

impl ProjectorSource {
    fn new(kind: SourceKind, topic: String, partitions: Vec<Arc<PartitionClient>>) -> Self {
        Self {
            kind,
            topic,
            partitions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    EngineInput,
    EngineReply,
    EngineEvent,
    WalletEvent,
}

impl fmt::Display for SourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineInput => f.write_str("engine input"),
            Self::EngineReply => f.write_str("engine reply"),
            Self::EngineEvent => f.write_str("engine event"),
            Self::WalletEvent => f.write_str("wallet event"),
        }
    }
}

async fn run_source_partition(
    kind: SourceKind,
    topic: String,
    partition_client: Arc<PartitionClient>,
    worker: ProjectorWorker,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = match worker.load_queue_offset(&topic, partition).await? {
        Some(offset) => offset,
        None => partition_client.get_offset(OffsetAt::Earliest).await?,
    };

    println!(
        "projector consuming {kind} '{topic}' partition {partition} from offset {next_offset}"
    );

    loop {
        let (records, high_watermark) = partition_client
            .fetch_records(next_offset, FETCH_BYTES, FETCH_MAX_WAIT_MS)
            .await?;

        if records.is_empty() {
            if next_offset >= high_watermark {
                tokio::time::sleep(IDLE_SLEEP).await;
            }
            continue;
        }

        for record in records {
            let offset = record.offset;
            let record_next_offset = offset + 1;
            let Some(payload) = record.record.value else {
                worker
                    .save_queue_offset(&topic, partition, record_next_offset)
                    .await?;
                next_offset = record_next_offset;
                continue;
            };

            match process_payload(
                kind,
                &topic,
                partition,
                offset,
                record_next_offset,
                payload,
                &worker,
            )
            .await
            {
                Ok(()) => {
                    next_offset = record_next_offset;
                }
                Err(error) if error.is_missing_order_context() => {
                    eprintln!(
                        "projector waiting for order context before {kind} {topic}[{partition}]@{offset}: {error}"
                    );
                    tokio::time::sleep(MISSING_CONTEXT_SLEEP).await;
                    break;
                }
                Err(error) => return Err(QueueError::Worker(error.to_string())),
            }
        }
    }
}

async fn process_payload(
    kind: SourceKind,
    topic: &str,
    partition: i32,
    offset: i64,
    next_offset: i64,
    payload: Vec<u8>,
    worker: &ProjectorWorker,
) -> Result<(), crate::worker::ProjectorError> {
    match kind {
        SourceKind::EngineInput => match serde_json::from_slice::<EngineInput>(&payload) {
            Ok(input) => {
                worker
                    .process_engine_input(input, topic, partition, next_offset)
                    .await
            }
            Err(error) => {
                skip_invalid_payload(topic, partition, offset, next_offset, error, worker).await
            }
        },
        SourceKind::EngineReply => match serde_json::from_slice::<EngineReply>(&payload) {
            Ok(reply) => {
                worker
                    .process_engine_reply(reply, topic, partition, next_offset)
                    .await
            }
            Err(error) => {
                skip_invalid_payload(topic, partition, offset, next_offset, error, worker).await
            }
        },
        SourceKind::EngineEvent => match serde_json::from_slice::<EngineEvent>(&payload) {
            Ok(event) => {
                worker
                    .process_engine_event(event, topic, partition, next_offset)
                    .await
            }
            Err(error) => {
                skip_invalid_payload(topic, partition, offset, next_offset, error, worker).await
            }
        },
        SourceKind::WalletEvent => match serde_json::from_slice::<WalletEvent>(&payload) {
            Ok(event) => {
                worker
                    .process_wallet_event(event, topic, partition, next_offset)
                    .await
            }
            Err(error) => {
                skip_invalid_payload(topic, partition, offset, next_offset, error, worker).await
            }
        },
    }
}

async fn skip_invalid_payload(
    topic: &str,
    partition: i32,
    offset: i64,
    next_offset: i64,
    error: serde_json::Error,
    worker: &ProjectorWorker,
) -> Result<(), crate::worker::ProjectorError> {
    eprintln!("invalid projector payload on {topic}[{partition}]@{offset}: {error}");
    worker
        .save_queue_offset(topic, partition, next_offset)
        .await
}

#[derive(Debug)]
pub enum QueueError {
    Client(ClientError),
    EmptyBrokerList,
    Task(String),
    TopicHasNoPartitions(String),
    TopicNotFound(String),
    Worker(String),
}

impl From<ClientError> for QueueError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<crate::worker::ProjectorError> for QueueError {
    fn from(error: crate::worker::ProjectorError) -> Self {
        Self::Worker(error.to_string())
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "redpanda client failed: {error}"),
            Self::EmptyBrokerList => write!(f, "redpanda broker list is empty"),
            Self::Task(error) => write!(f, "projector queue task failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
            Self::Worker(error) => write!(f, "projector worker failed: {error}"),
        }
    }
}

fn parse_brokers(brokers: &str) -> Result<Vec<String>, QueueError> {
    let brokers = brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(String::from)
        .collect::<Vec<_>>();

    if brokers.is_empty() {
        return Err(QueueError::EmptyBrokerList);
    }

    Ok(brokers)
}

async fn partition_clients(
    client: &Client,
    topics: &[Topic],
    topic: &str,
) -> Result<Vec<Arc<PartitionClient>>, QueueError> {
    let partitions = topic_partitions(topics, topic)?;
    let mut clients = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let client = client
            .partition_client(String::from(topic), partition, UnknownTopicHandling::Retry)
            .await?;
        clients.push(Arc::new(client));
    }

    Ok(clients)
}

fn topic_partitions(topics: &[Topic], topic: &str) -> Result<Vec<i32>, QueueError> {
    let partitions = topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .map(|topic| topic.partitions.iter().copied().collect::<Vec<_>>())
        .ok_or_else(|| QueueError::TopicNotFound(String::from(topic)))?;

    if partitions.is_empty() {
        return Err(QueueError::TopicHasNoPartitions(String::from(topic)));
    }

    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_brokers_ignores_empty_entries() {
        let brokers =
            parse_brokers("localhost:9092, ,redpanda:9092").expect("brokers should parse");

        assert_eq!(brokers, vec!["localhost:9092", "redpanda:9092"]);
    }

    #[test]
    fn parse_brokers_rejects_empty_entries() {
        assert!(matches!(
            parse_brokers(" , "),
            Err(QueueError::EmptyBrokerList)
        ));
    }

    #[test]
    fn source_kind_display_names_are_stable() {
        assert_eq!(SourceKind::EngineInput.to_string(), "engine input");
        assert_eq!(SourceKind::WalletEvent.to_string(), "wallet event");
    }
}
