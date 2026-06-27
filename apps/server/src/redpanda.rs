use std::{collections::BTreeMap, fmt, ops::Range, sync::Arc, time::Duration};

use chrono::Utc;
use protocol::{
    engine::EngineReply,
    wallet::{WalletCommand, WalletReply},
};
use rskafka::{
    client::{
        ClientBuilder,
        error::Error as ClientError,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
        producer::aggregator::RecordAggregator,
        producer::{BatchProducer, BatchProducerBuilder, Error as ProducerError},
    },
    record::Record,
    topic::Topic,
};

use crate::replies::ReplyState;

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct RedpandaProducer {
    wallet_commands_topic: String,
    wallet_commands: Vec<(i32, Arc<BatchProducer<RecordAggregator>>)>,
}

impl RedpandaProducer {
    pub async fn new(
        brokers: impl Into<String>,
        wallet_commands_topic: impl Into<String>,
    ) -> Result<Self, PublishError> {
        let brokers = parse_brokers(brokers.into())?;
        let wallet_commands_topic = wallet_commands_topic.into();
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-server")
            .build()
            .await?;
        let topics = client.list_topics().await?;
        let partitions = topics
            .iter()
            .find(|topic| topic.name == wallet_commands_topic)
            .map(|topic| topic.partitions.clone())
            .ok_or_else(|| PublishError::TopicNotFound(wallet_commands_topic.clone()))?;

        if partitions.is_empty() {
            return Err(PublishError::TopicHasNoPartitions(wallet_commands_topic));
        }

        let mut wallet_commands = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let partition_client = Arc::new(
                client
                    .partition_client(
                        wallet_commands_topic.clone(),
                        partition,
                        UnknownTopicHandling::Retry,
                    )
                    .await?,
            );
            let producer = BatchProducerBuilder::new(partition_client)
                .with_linger(Duration::from_millis(0))
                .build(RecordAggregator::new(1024 * 1024));
            wallet_commands.push((partition, Arc::new(producer)));
        }

        Ok(Self {
            wallet_commands_topic,
            wallet_commands,
        })
    }

    pub async fn publish_wallet_command(
        &self,
        key: &str,
        command: &WalletCommand,
    ) -> Result<ProducedRecord, PublishError> {
        let payload = serde_json::to_vec(command)?;
        let (partition, producer) = self.wallet_producer_for_key(key);
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let offset = producer.produce(record).await?;
        let produced = ProducedRecord { partition, offset };
        let trace = wallet_command_trace(command);
        println!(
            "server queued wallet command request_id={} idempotency_key={} topic={}[{}]@{}",
            trace.request_id.unwrap_or("-"),
            trace.idempotency_key.unwrap_or("-"),
            self.wallet_commands_topic,
            produced.partition,
            produced.offset
        );

        Ok(produced)
    }

    fn wallet_producer_for_key(&self, key: &str) -> (i32, &BatchProducer<RecordAggregator>) {
        let index = stable_partition(key.as_bytes(), self.wallet_commands.len());
        let (partition, producer) = &self.wallet_commands[index];
        (*partition, producer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducedRecord {
    pub partition: i32,
    pub offset: i64,
}

pub struct ReplyConsumers {
    wallet_replies_topic: String,
    wallet_replies: Arc<PartitionClient>,
    engine_replies_topic: String,
    engine_replies: Arc<PartitionClient>,
}

impl ReplyConsumers {
    pub async fn new(
        brokers: impl Into<String>,
        wallet_replies_topic: impl Into<String>,
        engine_replies_topic: impl Into<String>,
        reply_partition: i32,
    ) -> Result<Self, PublishError> {
        let brokers = parse_brokers(brokers.into())?;
        let wallet_replies_topic = wallet_replies_topic.into();
        let engine_replies_topic = engine_replies_topic.into();
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-server-replies")
            .build()
            .await?;
        let topics = client.list_topics().await?;

        ensure_topic_partition(&topics, &wallet_replies_topic, reply_partition)?;
        ensure_topic_partition(&topics, &engine_replies_topic, reply_partition)?;

        let wallet_replies = Arc::new(
            client
                .partition_client(
                    wallet_replies_topic.clone(),
                    reply_partition,
                    UnknownTopicHandling::Retry,
                )
                .await?,
        );
        let engine_replies = Arc::new(
            client
                .partition_client(
                    engine_replies_topic.clone(),
                    reply_partition,
                    UnknownTopicHandling::Retry,
                )
                .await?,
        );

        Ok(Self {
            wallet_replies_topic,
            wallet_replies,
            engine_replies_topic,
            engine_replies,
        })
    }

    pub fn spawn(self, reply_state: ReplyState) {
        let wallet_state = reply_state.clone();
        tokio::spawn(async move {
            if let Err(error) =
                consume_wallet_replies(self.wallet_replies_topic, self.wallet_replies, wallet_state)
                    .await
            {
                eprintln!("{error}");
            }
        });

        tokio::spawn(async move {
            if let Err(error) =
                consume_engine_replies(self.engine_replies_topic, self.engine_replies, reply_state)
                    .await
            {
                eprintln!("{error}");
            }
        });
    }
}

async fn consume_wallet_replies(
    topic: String,
    partition_client: Arc<PartitionClient>,
    reply_state: ReplyState,
) -> Result<(), PublishError> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client.get_offset(OffsetAt::Latest).await?;

    println!("server consuming '{topic}' partition {partition} from offset {next_offset}");

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

            match serde_json::from_slice::<WalletReply>(&payload) {
                Ok(reply) => {
                    println!(
                        "server received wallet reply request_id={} source={topic}[{partition}]@{}",
                        wallet_reply_request_id(&reply),
                        record.offset
                    );
                    reply_state.resolve_wallet_reply(reply).await
                }
                Err(error) => eprintln!(
                    "invalid wallet reply on {topic}[{partition}]@{}: {error}",
                    record.offset
                ),
            }
        }
    }
}

async fn consume_engine_replies(
    topic: String,
    partition_client: Arc<PartitionClient>,
    reply_state: ReplyState,
) -> Result<(), PublishError> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client.get_offset(OffsetAt::Latest).await?;

    println!("server consuming '{topic}' partition {partition} from offset {next_offset}");

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

            match serde_json::from_slice::<EngineReply>(&payload) {
                Ok(reply) => {
                    let trace = engine_reply_trace(&reply);
                    println!(
                        "server received engine reply request_id={} source={topic}[{partition}]@{} source_input_id={} source_input_offset={}",
                        trace.request_id,
                        record.offset,
                        trace.source_input_id.unwrap_or("-"),
                        trace
                            .source_input_offset
                            .map(|offset| offset.to_string())
                            .as_deref()
                            .unwrap_or("-")
                    );
                    reply_state.resolve_engine_reply(reply).await
                }
                Err(error) => eprintln!(
                    "invalid engine reply on {topic}[{partition}]@{}: {error}",
                    record.offset
                ),
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WalletCommandTrace<'a> {
    request_id: Option<&'a str>,
    idempotency_key: Option<&'a str>,
}

fn wallet_command_trace(command: &WalletCommand) -> WalletCommandTrace<'_> {
    match command {
        WalletCommand::PlaceOrderIntent(command) => WalletCommandTrace {
            request_id: Some(command.envelope.request_id.as_str()),
            idempotency_key: Some(command.envelope.idempotency_key.as_str()),
        },
        WalletCommand::CancelOrderIntent(command) => WalletCommandTrace {
            request_id: Some(command.envelope.request_id.as_str()),
            idempotency_key: Some(command.envelope.idempotency_key.as_str()),
        },
        WalletCommand::Deposit(command) => WalletCommandTrace {
            request_id: Some(command.envelope.request_id.as_str()),
            idempotency_key: Some(command.envelope.idempotency_key.as_str()),
        },
        WalletCommand::Withdraw(command) => WalletCommandTrace {
            request_id: Some(command.envelope.request_id.as_str()),
            idempotency_key: Some(command.envelope.idempotency_key.as_str()),
        },
        WalletCommand::ReleaseReservation(_) | WalletCommand::SettleTrade(_) => {
            WalletCommandTrace {
                request_id: None,
                idempotency_key: None,
            }
        }
    }
}

fn wallet_reply_request_id(reply: &WalletReply) -> &str {
    match reply {
        WalletReply::FundsReserved(reply) => reply.request_id.as_str(),
        WalletReply::InsufficientFunds(reply) => reply.request_id.as_str(),
        WalletReply::BalanceUpdated(reply) => reply.request_id.as_str(),
        WalletReply::CommandAccepted(reply) => reply.request_id.as_str(),
        WalletReply::CommandRejected(reply) => reply.request_id.as_str(),
    }
}

#[derive(Debug, Clone, Copy)]
struct EngineReplyTrace<'a> {
    request_id: &'a str,
    source_input_id: Option<&'a str>,
    source_input_offset: Option<i64>,
}

fn engine_reply_trace(reply: &EngineReply) -> EngineReplyTrace<'_> {
    match reply {
        EngineReply::OrderAccepted(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
        EngineReply::OrderRejected(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
        EngineReply::CancelAccepted(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
        EngineReply::CancelRejected(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
        EngineReply::LiquidationAccepted(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
        EngineReply::LiquidationRejected(reply) => EngineReplyTrace {
            request_id: reply.request_id.as_str(),
            source_input_id: reply.source_input_id.as_deref(),
            source_input_offset: reply.source_input_offset,
        },
    }
}

#[derive(Debug)]
pub enum PublishError {
    Client(ClientError),
    EmptyBrokerList,
    Producer(ProducerError),
    Serialize(serde_json::Error),
    TopicHasNoPartitions(String),
    TopicNotFound(String),
}

impl From<ClientError> for PublishError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<ProducerError> for PublishError {
    fn from(error: ProducerError) -> Self {
        Self::Producer(error)
    }
}

impl From<serde_json::Error> for PublishError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "redpanda client failed: {error}"),
            Self::EmptyBrokerList => write!(f, "redpanda broker list is empty"),
            Self::Producer(error) => write!(f, "redpanda publish failed: {error}"),
            Self::Serialize(error) => write!(f, "command serialization failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
        }
    }
}

fn parse_brokers(brokers: String) -> Result<Vec<String>, PublishError> {
    let brokers = brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(String::from)
        .collect::<Vec<_>>();

    if brokers.is_empty() {
        return Err(PublishError::EmptyBrokerList);
    }

    Ok(brokers)
}

fn ensure_topic_partition(
    topics: &[Topic],
    topic: &str,
    partition: i32,
) -> Result<(), PublishError> {
    let partitions = topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .map(|topic| topic.partitions.clone())
        .ok_or_else(|| PublishError::TopicNotFound(String::from(topic)))?;

    if partitions.is_empty() {
        return Err(PublishError::TopicHasNoPartitions(String::from(topic)));
    }
    if !partitions.contains(&partition) {
        return Err(PublishError::TopicNotFound(format!(
            "{topic} partition {partition}"
        )));
    }

    Ok(())
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
            parse_brokers(String::from("localhost:9092, ,redpanda:9092")).expect("valid brokers");

        assert_eq!(brokers, vec!["localhost:9092", "redpanda:9092"]);
    }

    #[test]
    fn parse_brokers_rejects_empty_list() {
        assert!(matches!(
            parse_brokers(String::from(" , ")),
            Err(PublishError::EmptyBrokerList)
        ));
    }

    #[test]
    fn stable_partition_keeps_same_key_on_same_partition() {
        let first = stable_partition(b"user-42", 3);
        let second = stable_partition(b"user-42", 3);

        assert_eq!(first, second);
        assert!(first < 3);
    }
}
