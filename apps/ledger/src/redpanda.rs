use std::{fmt, ops::Range, sync::Arc, time::Duration};

use protocol::wallet::WalletEvent;
use rskafka::{
    client::{
        Client, ClientBuilder,
        error::Error as ClientError,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    topic::Topic,
};
use tokio::task::JoinSet;

use crate::{settings::LedgerSettings, worker::LedgerWorker};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);

pub struct LedgerQueue {
    wallet_events_topic: String,
    wallet_event_partitions: Vec<Arc<PartitionClient>>,
}

impl LedgerQueue {
    pub async fn new(settings: &LedgerSettings) -> Result<Self, QueueError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-ledger")
            .build()
            .await?;
        let topics = client.list_topics().await?;
        let wallet_event_partitions =
            partition_clients(&client, &topics, &settings.wallet_events_topic).await?;

        Ok(Self {
            wallet_events_topic: settings.wallet_events_topic.clone(),
            wallet_event_partitions,
        })
    }

    pub async fn run(self, worker: LedgerWorker) -> Result<(), QueueError> {
        let mut tasks = JoinSet::new();

        for partition_client in self.wallet_event_partitions {
            let topic = self.wallet_events_topic.clone();
            let worker = worker.clone();

            tasks.spawn(async move {
                run_wallet_event_partition(topic, partition_client, worker).await
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
    worker: LedgerWorker,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = match worker.load_queue_offset(&topic, partition).await? {
        Some(offset) => offset,
        None => partition_client.get_offset(OffsetAt::Earliest).await?,
    };

    println!("ledger consuming '{topic}' partition {partition} from offset {next_offset}");

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

            let event = match serde_json::from_slice::<WalletEvent>(&payload) {
                Ok(event) => event,
                Err(error) => {
                    eprintln!("invalid wallet event on {topic}[{partition}]@{offset}: {error}");
                    worker
                        .save_queue_offset(&topic, partition, record_next_offset)
                        .await?;
                    next_offset = record_next_offset;
                    continue;
                }
            };

            worker
                .process_wallet_event(event, &topic, partition, offset, record_next_offset)
                .await?;
            next_offset = record_next_offset;
        }
    }
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

impl From<crate::worker::LedgerError> for QueueError {
    fn from(error: crate::worker::LedgerError) -> Self {
        Self::Worker(error.to_string())
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "redpanda client failed: {error}"),
            Self::EmptyBrokerList => write!(f, "redpanda broker list is empty"),
            Self::Task(error) => write!(f, "ledger queue task failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
            Self::Worker(error) => write!(f, "ledger worker failed: {error}"),
        }
    }
}

impl std::error::Error for QueueError {}

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
}
