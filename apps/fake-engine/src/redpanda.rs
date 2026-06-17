use std::{collections::BTreeMap, error::Error, fmt, ops::Range, sync::Arc, time::Duration};

use chrono::Utc;
use protocol::{engine::EngineCommand, wallet::WalletEvent};
use rskafka::{
    client::{
        Client, ClientBuilder,
        error::Error as ClientError,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
        producer::{
            BatchProducer, BatchProducerBuilder, Error as ProducerError,
            aggregator::RecordAggregator,
        },
    },
    record::Record,
    topic::Topic,
};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::{
    engine::{FakeEngine, FakeEngineOutput},
    settings::FakeEngineSettings,
};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);
const PRODUCER_BATCH_BYTES: usize = 1024 * 1024;

pub struct FakeEngineQueue {
    engine_commands_topic: String,
    command_partitions: Vec<Arc<PartitionClient>>,
    wallet_events_topic: String,
    wallet_event_partitions: Vec<Arc<PartitionClient>>,
    publishers: EnginePublishers,
}

impl FakeEngineQueue {
    pub async fn new(settings: &FakeEngineSettings) -> Result<Self, QueueError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-fake-engine")
            .build()
            .await?;
        let topics = client.list_topics().await?;

        let command_partitions =
            partition_clients(&client, &topics, &settings.engine_commands_topic).await?;
        let wallet_event_partitions =
            partition_clients(&client, &topics, &settings.wallet_events_topic).await?;
        let publishers = EnginePublishers {
            engine_replies: TopicProducer::new(
                &client,
                &topics,
                settings.engine_replies_topic.clone(),
            )
            .await?,
            engine_events: TopicProducer::new(
                &client,
                &topics,
                settings.engine_events_topic.clone(),
            )
            .await?,
        };

        Ok(Self {
            engine_commands_topic: settings.engine_commands_topic.clone(),
            command_partitions,
            wallet_events_topic: settings.wallet_events_topic.clone(),
            wallet_event_partitions,
            publishers,
        })
    }

    pub async fn run(self, engine: FakeEngine) -> Result<(), QueueError> {
        let mut tasks = JoinSet::new();

        for partition_client in self.wallet_event_partitions {
            let topic = self.wallet_events_topic.clone();
            let engine = engine.clone();

            tasks.spawn(async move {
                run_wallet_event_partition(topic, partition_client, engine).await
            });
        }

        for partition_client in self.command_partitions {
            let topic = self.engine_commands_topic.clone();
            let engine = engine.clone();
            let publishers = self.publishers.clone();

            tasks.spawn(async move {
                run_command_partition(topic, partition_client, engine, publishers).await
            });
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

async fn run_wallet_event_partition(
    topic: String,
    partition_client: Arc<PartitionClient>,
    engine: FakeEngine,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client.get_offset(OffsetAt::Latest).await?;

    println!("fake engine observing '{topic}' partition {partition} from offset {next_offset}");

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
            next_offset = record.offset + 1;
            let Some(payload) = record.record.value else {
                continue;
            };

            match serde_json::from_slice::<WalletEvent>(&payload) {
                Ok(event) => engine.observe_wallet_event(event),
                Err(error) => eprintln!(
                    "invalid wallet event on {topic}[{partition}]@{}: {error}",
                    record.offset
                ),
            }
        }
    }
}

async fn run_command_partition(
    topic: String,
    partition_client: Arc<PartitionClient>,
    engine: FakeEngine,
    publishers: EnginePublishers,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client.get_offset(OffsetAt::Latest).await?;

    println!("fake engine consuming '{topic}' partition {partition} from offset {next_offset}");

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
            next_offset = record.offset + 1;
            let Some(payload) = record.record.value else {
                continue;
            };

            match serde_json::from_slice::<EngineCommand>(&payload) {
                Ok(command) => {
                    let output = engine.process_command(command);
                    publishers.publish_output(output).await?;
                }
                Err(error) => eprintln!(
                    "invalid engine command on {topic}[{partition}]@{}: {error}",
                    record.offset
                ),
            }
        }
    }
}

#[derive(Clone)]
struct EnginePublishers {
    engine_replies: TopicProducer,
    engine_events: TopicProducer,
}

impl EnginePublishers {
    async fn publish_output(&self, output: FakeEngineOutput) -> Result<(), QueueError> {
        for reply in output.replies {
            self.engine_replies
                .publish_json_to_partition(reply.partition, &reply.key, &reply.reply)
                .await?;
        }

        for event in output.events {
            self.engine_events
                .publish_json(&event.key, &event.event)
                .await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct TopicProducer {
    topic: String,
    producers: Vec<(i32, Arc<BatchProducer<RecordAggregator>>)>,
}

impl TopicProducer {
    async fn new(client: &Client, topics: &[Topic], topic: String) -> Result<Self, QueueError> {
        let partitions = topic_partitions(topics, &topic)?;
        let mut producers = Vec::with_capacity(partitions.len());

        for partition in partitions {
            let partition_client = Arc::new(
                client
                    .partition_client(topic.clone(), partition, UnknownTopicHandling::Retry)
                    .await?,
            );
            let producer = BatchProducerBuilder::new(partition_client)
                .with_linger(Duration::from_millis(0))
                .build(RecordAggregator::new(PRODUCER_BATCH_BYTES));
            producers.push((partition, Arc::new(producer)));
        }

        Ok(Self { topic, producers })
    }

    async fn publish_json<T: Serialize>(&self, key: &str, value: &T) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(value)?;
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };
        let producer = self.producer_for_key(key);

        producer.produce(record).await?;
        Ok(())
    }

    async fn publish_json_to_partition<T: Serialize>(
        &self,
        partition: i32,
        key: &str,
        value: &T,
    ) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(value)?;
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };
        let producer = self.producer_for_partition(partition)?;

        producer.produce(record).await?;
        Ok(())
    }

    fn producer_for_key(&self, key: &str) -> &BatchProducer<RecordAggregator> {
        let index = stable_partition(key.as_bytes(), self.producers.len());
        &self.producers[index].1
    }

    fn producer_for_partition(
        &self,
        partition: i32,
    ) -> Result<&BatchProducer<RecordAggregator>, QueueError> {
        self.producers
            .iter()
            .find(|(candidate, _)| *candidate == partition)
            .map(|(_, producer)| producer.as_ref())
            .ok_or_else(|| QueueError::PartitionNotFound {
                topic: self.topic.clone(),
                partition,
            })
    }
}

#[derive(Debug)]
pub enum QueueError {
    Client(ClientError),
    EmptyBrokerList,
    PartitionNotFound { topic: String, partition: i32 },
    Producer(ProducerError),
    Serialize(serde_json::Error),
    Task(String),
    TopicHasNoPartitions(String),
    TopicNotFound(String),
}

impl From<ClientError> for QueueError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<ProducerError> for QueueError {
    fn from(error: ProducerError) -> Self {
        Self::Producer(error)
    }
}

impl From<serde_json::Error> for QueueError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "redpanda client failed: {error}"),
            Self::EmptyBrokerList => write!(f, "redpanda broker list is empty"),
            Self::PartitionNotFound { topic, partition } => {
                write!(f, "redpanda topic '{topic}' has no partition {partition}")
            }
            Self::Producer(error) => write!(f, "redpanda publish failed: {error}"),
            Self::Serialize(error) => write!(f, "redpanda payload serialization failed: {error}"),
            Self::Task(error) => write!(f, "fake engine queue task failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
        }
    }
}

impl Error for QueueError {}

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

fn stable_partition(key: &[u8], partition_count: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    (hash as usize) % partition_count
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
    fn stable_partition_keeps_key_on_same_partition() {
        let first = stable_partition(b"market-1", 3);
        let second = stable_partition(b"market-1", 3);

        assert_eq!(first, second);
        assert!(first < 3);
    }
}
