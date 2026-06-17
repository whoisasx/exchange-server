use std::{error::Error, fmt, ops::Range, sync::Arc, time::Duration};

use protocol::{engine::EngineEvent, wallet::WalletEvent};
use rskafka::{
    client::{
        Client, ClientBuilder,
        error::Error as ClientError,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    topic::Topic,
};
use tokio::task::JoinSet;

use crate::{messages::StreamMetadata, router::EventRouter, settings::WsSettings};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);

pub struct EventConsumers {
    sources: Vec<EventSource>,
    router: EventRouter,
}

impl EventConsumers {
    pub async fn new(settings: &WsSettings, router: EventRouter) -> Result<Self, ConsumerError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-ws")
            .build()
            .await?;
        let topics = client.list_topics().await?;

        let sources = vec![
            EventSource::new(
                SourceKind::EngineEvent,
                settings.engine_events_topic.clone(),
                partition_clients(&client, &topics, &settings.engine_events_topic).await?,
            ),
            EventSource::new(
                SourceKind::WalletEvent,
                settings.wallet_events_topic.clone(),
                partition_clients(&client, &topics, &settings.wallet_events_topic).await?,
            ),
        ];

        Ok(Self { sources, router })
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            if let Err(error) = self.run().await {
                eprintln!("{error}");
            }
        });
    }

    async fn run(self) -> Result<(), ConsumerError> {
        let mut tasks = JoinSet::new();

        for source in self.sources {
            for partition_client in source.partitions {
                let topic = source.topic.clone();
                let router = self.router.clone();
                let kind = source.kind;

                tasks.spawn(async move {
                    run_event_partition(kind, topic, partition_client, router).await
                });
            }
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(ConsumerError::Task(error.to_string())),
            }
        }

        Ok(())
    }
}

struct EventSource {
    kind: SourceKind,
    topic: String,
    partitions: Vec<Arc<PartitionClient>>,
}

impl EventSource {
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
    EngineEvent,
    WalletEvent,
}

impl fmt::Display for SourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineEvent => f.write_str("engine event"),
            Self::WalletEvent => f.write_str("wallet event"),
        }
    }
}

async fn run_event_partition(
    kind: SourceKind,
    topic: String,
    partition_client: Arc<PartitionClient>,
    router: EventRouter,
) -> Result<(), ConsumerError> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client.get_offset(OffsetAt::Latest).await?;

    println!("ws consuming {kind} '{topic}' partition {partition} from offset {next_offset}");

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
            next_offset = offset + 1;
            let Some(payload) = record.record.value else {
                continue;
            };
            let metadata = StreamMetadata {
                topic: topic.clone(),
                partition,
                offset,
            };

            match kind {
                SourceKind::EngineEvent => match serde_json::from_slice::<EngineEvent>(&payload) {
                    Ok(event) => {
                        if let Err(error) = router.process_engine_event(event, metadata).await {
                            eprintln!(
                                "failed to route engine event on {topic}[{partition}]@{offset}: {error}"
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!("invalid engine event on {topic}[{partition}]@{offset}: {error}");
                    }
                },
                SourceKind::WalletEvent => match serde_json::from_slice::<WalletEvent>(&payload) {
                    Ok(event) => {
                        if let Err(error) = router.process_wallet_event(event, metadata).await {
                            eprintln!(
                                "failed to route wallet event on {topic}[{partition}]@{offset}: {error}"
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!("invalid wallet event on {topic}[{partition}]@{offset}: {error}");
                    }
                },
            }
        }
    }
}

#[derive(Debug)]
pub enum ConsumerError {
    Client(ClientError),
    EmptyBrokerList,
    Task(String),
    TopicHasNoPartitions(String),
    TopicNotFound(String),
}

impl From<ClientError> for ConsumerError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "redpanda client failed: {error}"),
            Self::EmptyBrokerList => write!(f, "redpanda broker list is empty"),
            Self::Task(error) => write!(f, "websocket consumer task failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
        }
    }
}

impl Error for ConsumerError {}

fn parse_brokers(brokers: &str) -> Result<Vec<String>, ConsumerError> {
    let brokers = brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(String::from)
        .collect::<Vec<_>>();

    if brokers.is_empty() {
        return Err(ConsumerError::EmptyBrokerList);
    }

    Ok(brokers)
}

async fn partition_clients(
    client: &Client,
    topics: &[Topic],
    topic: &str,
) -> Result<Vec<Arc<PartitionClient>>, ConsumerError> {
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

fn topic_partitions(topics: &[Topic], topic: &str) -> Result<Vec<i32>, ConsumerError> {
    let partitions = topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .map(|topic| topic.partitions.iter().copied().collect::<Vec<_>>())
        .ok_or_else(|| ConsumerError::TopicNotFound(String::from(topic)))?;

    if partitions.is_empty() {
        return Err(ConsumerError::TopicHasNoPartitions(String::from(topic)));
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
            Err(ConsumerError::EmptyBrokerList)
        ));
    }
}
