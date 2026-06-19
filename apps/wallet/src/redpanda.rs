use std::{collections::BTreeMap, fmt, ops::Range, sync::Arc, time::Duration};

use chrono::Utc;
use protocol::{
    engine::{
        EngineEvent, FundingRateUpdatedInput, FundingSettlementTickInput, MarkPriceUpdatedInput,
    },
    wallet::{ReleaseReservation, SettleTrade, WalletCommand},
};
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
    engine_inputs::EngineInputPublication,
    processor::{ApplyAccountDelta, EngineWalletCommand},
    settings::WalletSettings,
    worker::WalletWorker,
};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 500;
const IDLE_SLEEP: Duration = Duration::from_millis(100);
const PRODUCER_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct WalletEngineInputPublisher {
    engine_inputs: TopicProducer,
}

impl WalletEngineInputPublisher {
    pub async fn new(settings: &WalletSettings) -> Result<Self, QueueError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-wallet-engine-input-publisher")
            .build()
            .await?;
        let topics = client.list_topics().await?;
        let engine_inputs =
            TopicProducer::new(&client, &topics, settings.engine_commands_topic.clone()).await?;

        Ok(Self { engine_inputs })
    }

    pub async fn publish_mark_price_updated(
        &self,
        input: MarkPriceUpdatedInput,
    ) -> Result<(), QueueError> {
        self.publish(EngineInputPublication::mark_price_updated(input))
            .await
    }

    pub async fn publish_funding_rate_updated(
        &self,
        input: FundingRateUpdatedInput,
    ) -> Result<(), QueueError> {
        self.publish(EngineInputPublication::funding_rate_updated(input))
            .await
    }

    pub async fn publish_funding_settlement_tick(
        &self,
        input: FundingSettlementTickInput,
    ) -> Result<(), QueueError> {
        self.publish(EngineInputPublication::funding_settlement_tick(input))
            .await
    }

    pub async fn publish(&self, publication: EngineInputPublication) -> Result<(), QueueError> {
        self.engine_inputs
            .publish_json(publication.key(), publication.input())
            .await
    }
}

pub struct WalletQueue {
    wallet_commands_topic: String,
    command_partitions: Vec<Arc<PartitionClient>>,
    engine_events_topic: String,
    engine_event_partitions: Vec<Arc<PartitionClient>>,
    publishers: WalletPublishers,
}

impl WalletQueue {
    pub async fn new(settings: &WalletSettings) -> Result<Self, QueueError> {
        let brokers = parse_brokers(&settings.redpanda_brokers)?;
        let client = ClientBuilder::new(brokers)
            .client_id("exchange-wallet")
            .build()
            .await?;
        let topics = client.list_topics().await?;

        let command_partitions =
            partition_clients(&client, &topics, &settings.wallet_commands_topic).await?;
        let engine_event_partitions =
            partition_clients(&client, &topics, &settings.engine_events_topic).await?;
        let publishers = WalletPublishers {
            wallet_replies: TopicProducer::new(
                &client,
                &topics,
                settings.wallet_replies_topic.clone(),
            )
            .await?,
            wallet_events: TopicProducer::new(
                &client,
                &topics,
                settings.wallet_events_topic.clone(),
            )
            .await?,
            engine_commands: TopicProducer::new(
                &client,
                &topics,
                settings.engine_commands_topic.clone(),
            )
            .await?,
        };

        Ok(Self {
            wallet_commands_topic: settings.wallet_commands_topic.clone(),
            command_partitions,
            engine_events_topic: settings.engine_events_topic.clone(),
            engine_event_partitions,
            publishers,
        })
    }

    pub async fn run(self, worker: WalletWorker) -> Result<(), QueueError> {
        let mut tasks = JoinSet::new();

        for partition_client in self.command_partitions {
            let topic = self.wallet_commands_topic.clone();
            let publishers = self.publishers.clone();
            let worker = worker.clone();

            tasks.spawn(async move {
                run_command_partition(topic, partition_client, publishers, worker).await
            });
        }

        for partition_client in self.engine_event_partitions {
            let topic = self.engine_events_topic.clone();
            let publishers = self.publishers.clone();
            let worker = worker.clone();

            tasks.spawn(async move {
                run_engine_event_partition(topic, partition_client, publishers, worker).await
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

async fn run_command_partition(
    topic: String,
    partition_client: Arc<PartitionClient>,
    publishers: WalletPublishers,
    worker: WalletWorker,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = match worker.load_queue_offset(&topic, partition).await? {
        Some(offset) => offset,
        None => partition_client.get_offset(OffsetAt::Earliest).await?,
    };

    println!("wallet queue consuming '{topic}' partition {partition} from offset {next_offset}");

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
            let record_next_offset = record.offset + 1;
            let Some(payload) = record.record.value else {
                worker
                    .save_queue_offset(&topic, partition, record_next_offset)
                    .await?;
                next_offset = record_next_offset;
                continue;
            };

            let command = match serde_json::from_slice::<WalletCommand>(&payload) {
                Ok(command) => command,
                Err(error) => {
                    eprintln!(
                        "invalid wallet command on {topic}[{partition}]@{}: {error}",
                        record.offset
                    );
                    worker
                        .save_queue_offset(&topic, partition, record_next_offset)
                        .await?;
                    next_offset = record_next_offset;
                    continue;
                }
            };

            let metadata = CommandMetadata::from_command(&command);
            let result = worker.process_command(command).await?;
            publishers.publish_result(metadata, result).await?;
            worker
                .save_queue_offset(&topic, partition, record_next_offset)
                .await?;
            next_offset = record_next_offset;
        }
    }
}

async fn run_engine_event_partition(
    topic: String,
    partition_client: Arc<PartitionClient>,
    publishers: WalletPublishers,
    worker: WalletWorker,
) -> Result<(), QueueError> {
    let partition = partition_client.partition();
    let mut next_offset = match worker.load_queue_offset(&topic, partition).await? {
        Some(offset) => offset,
        None => partition_client.get_offset(OffsetAt::Earliest).await?,
    };

    println!("wallet queue consuming '{topic}' partition {partition} from offset {next_offset}");

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
            let record_next_offset = record.offset + 1;
            let Some(payload) = record.record.value else {
                worker
                    .save_queue_offset(&topic, partition, record_next_offset)
                    .await?;
                next_offset = record_next_offset;
                continue;
            };

            let event = match serde_json::from_slice::<EngineEvent>(&payload) {
                Ok(event) => event,
                Err(error) => {
                    eprintln!(
                        "invalid engine event on {topic}[{partition}]@{}: {error}",
                        record.offset
                    );
                    worker
                        .save_queue_offset(&topic, partition, record_next_offset)
                        .await?;
                    next_offset = record_next_offset;
                    continue;
                }
            };

            for command in engine_wallet_commands_from_engine_event(event) {
                let metadata = CommandMetadata::from_engine_wallet_command(&command);
                let result = worker.process_engine_command(command).await?;
                publishers.publish_result(metadata, result).await?;
            }

            worker
                .save_queue_offset(&topic, partition, record_next_offset)
                .await?;
            next_offset = record_next_offset;
        }
    }
}

#[derive(Clone)]
struct WalletPublishers {
    wallet_replies: TopicProducer,
    wallet_events: TopicProducer,
    engine_commands: TopicProducer,
}

impl WalletPublishers {
    async fn publish_result(
        &self,
        metadata: CommandMetadata,
        result: crate::processor::WalletProcessResult,
    ) -> Result<(), QueueError> {
        for reply in result.wallet_replies {
            let Some(reply_partition) = metadata.reply_partition else {
                return Err(QueueError::MissingReplyPartition(metadata.reply_key));
            };
            self.wallet_replies
                .publish_json_to_partition(reply_partition, &metadata.reply_key, &reply)
                .await?;
        }

        for event in result.wallet_events {
            self.wallet_events
                .publish_json(&metadata.wallet_key, &event)
                .await?;
        }

        for command in result.engine_commands {
            let publication = EngineInputPublication::new(command);
            self.engine_commands
                .publish_json(publication.key(), publication.input())
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandMetadata {
    wallet_key: String,
    reply_key: String,
    reply_partition: Option<i32>,
}

impl CommandMetadata {
    fn from_command(command: &WalletCommand) -> Self {
        match command {
            WalletCommand::PlaceOrderIntent(command) => Self {
                wallet_key: command.envelope.user_id.to_string(),
                reply_key: command.envelope.request_id.clone(),
                reply_partition: Some(command.envelope.reply_partition),
            },
            WalletCommand::CancelOrderIntent(command) => Self {
                wallet_key: command.envelope.user_id.to_string(),
                reply_key: command.envelope.request_id.clone(),
                reply_partition: Some(command.envelope.reply_partition),
            },
            WalletCommand::Deposit(command) => Self {
                wallet_key: command.envelope.user_id.to_string(),
                reply_key: command.envelope.request_id.clone(),
                reply_partition: Some(command.envelope.reply_partition),
            },
            WalletCommand::Withdraw(command) => Self {
                wallet_key: command.envelope.user_id.to_string(),
                reply_key: command.envelope.request_id.clone(),
                reply_partition: Some(command.envelope.reply_partition),
            },
            WalletCommand::ReleaseReservation(command) => Self {
                wallet_key: command.reservation_id.clone(),
                reply_key: command.reservation_id.clone(),
                reply_partition: None,
            },
            WalletCommand::SettleTrade(command) => Self {
                wallet_key: command.reservation_id.clone(),
                reply_key: command.fill_id.to_string(),
                reply_partition: None,
            },
        }
    }

    fn from_engine_wallet_command(command: &EngineWalletCommand) -> Self {
        match command {
            EngineWalletCommand::ReleaseReservation(command) => Self {
                wallet_key: command.reservation_id.clone(),
                reply_key: command.reservation_id.clone(),
                reply_partition: None,
            },
            EngineWalletCommand::SettleTrade(command) => Self {
                wallet_key: command.reservation_id.clone(),
                reply_key: command.fill_id.to_string(),
                reply_partition: None,
            },
            EngineWalletCommand::ApplyAccountDelta(command) => Self {
                wallet_key: command.user_id.to_string(),
                reply_key: command.reference_id.clone(),
                reply_partition: None,
            },
        }
    }
}

#[derive(Debug)]
pub enum QueueError {
    Client(ClientError),
    EmptyBrokerList,
    MissingReplyPartition(String),
    PartitionNotFound { topic: String, partition: i32 },
    Producer(ProducerError),
    Repository(String),
    Serialize(serde_json::Error),
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
            Self::MissingReplyPartition(request_id) => {
                write!(f, "wallet reply for '{request_id}' has no reply partition")
            }
            Self::PartitionNotFound { topic, partition } => {
                write!(f, "redpanda topic '{topic}' has no partition {partition}")
            }
            Self::Producer(error) => write!(f, "redpanda publish failed: {error}"),
            Self::Repository(error) => write!(f, "wallet offset storage failed: {error}"),
            Self::Serialize(error) => write!(f, "redpanda payload serialization failed: {error}"),
            Self::Task(error) => write!(f, "wallet queue task failed: {error}"),
            Self::TopicHasNoPartitions(topic) => {
                write!(f, "redpanda topic '{topic}' has no partitions")
            }
            Self::TopicNotFound(topic) => write!(f, "redpanda topic '{topic}' was not found"),
            Self::Worker(error) => write!(f, "wallet processor failed: {error}"),
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

fn engine_wallet_commands_from_engine_event(event: EngineEvent) -> Vec<EngineWalletCommand> {
    match event {
        EngineEvent::OrderCancelled(event) => release_reservation_command(
            event.reservation_id,
            event.released_amount,
            format!("order_cancelled:{}", event.order_id),
        ),
        EngineEvent::OrderExpired(event) => release_reservation_command(
            event.reservation_id,
            event.released_amount,
            format!("order_expired:{}:{}", event.order_id, event.reason),
        ),
        EngineEvent::ReservationReleased(event) => release_reservation_command(
            event.reservation_id,
            event.released_amount,
            format!("reservation_released:{}", event.reason),
        ),
        EngineEvent::TradeExecuted(event) => {
            let fill_id = event.fill_id;
            let mut commands = event
                .settlements
                .into_iter()
                .map(|settlement| {
                    EngineWalletCommand::SettleTrade(SettleTrade {
                        fill_id,
                        reservation_id: settlement.reservation_id,
                        debit_asset: settlement.debit_asset,
                        debit_amount: settlement.debit_amount,
                        credit_asset: settlement.credit_asset,
                        credit_amount: settlement.credit_amount,
                    })
                })
                .collect::<Vec<_>>();

            commands.extend(
                event
                    .fee_deltas
                    .into_iter()
                    .enumerate()
                    .map(|(index, fee)| {
                        EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                            user_id: fee.user_id,
                            asset: fee.asset,
                            total_delta: -fee.amount,
                            locked_delta: 0,
                            kind: String::from("TRADE_FEE"),
                            reference_id: format!(
                                "fill:{fill_id}:fee:{index}:{}:{}",
                                fee.user_id, fee.fee_type
                            ),
                        })
                    }),
            );

            commands
        }
        EngineEvent::FundingPaymentApplied(event) => {
            let event_reference = event.engine_event_id.unwrap_or_else(|| {
                format!("funding:{}:{}", event.market_id, event.engine_sequence)
            });
            event
                .payments
                .into_iter()
                .enumerate()
                .map(|(index, payment)| {
                    EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                        user_id: payment.user_id,
                        asset: payment.asset,
                        total_delta: payment.amount,
                        locked_delta: 0,
                        kind: String::from("FUNDING_PAYMENT"),
                        reference_id: format!(
                            "{event_reference}:payment:{index}:{}",
                            payment.position_id
                        ),
                    })
                })
                .collect()
        }
        EngineEvent::FeeCharged(event) => {
            vec![EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                user_id: event.user_id,
                asset: event.asset,
                total_delta: -event.amount,
                locked_delta: 0,
                kind: String::from("FEE_CHARGED"),
                reference_id: event.fee_id,
            })]
        }
        EngineEvent::AccountDelta(event) => {
            vec![EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                user_id: event.user_id,
                asset: event.asset,
                total_delta: event.total_delta,
                locked_delta: event.locked_delta,
                kind: String::from("ACCOUNT_DELTA"),
                reference_id: event.account_delta_id,
            })]
        }
        EngineEvent::OrderOpened(_)
        | EngineEvent::OrderBookDelta(_)
        | EngineEvent::MarkPriceUpdated(_)
        | EngineEvent::FundingRateUpdated(_)
        | EngineEvent::PositionChanged(_)
        | EngineEvent::RiskStateUpdated(_)
        | EngineEvent::LiquidationStarted(_)
        | EngineEvent::LiquidationExecuted(_)
        | EngineEvent::LiquidationCompleted(_)
        | EngineEvent::AdlExecuted(_)
        | EngineEvent::OrderBookSnapshotCreated(_)
        | EngineEvent::EngineCheckpointCommitted(_) => Vec::new(),
    }
}

fn release_reservation_command(
    reservation_id: String,
    amount: i64,
    reason: String,
) -> Vec<EngineWalletCommand> {
    if amount <= 0 {
        return Vec::new();
    }

    vec![EngineWalletCommand::ReleaseReservation(
        ReleaseReservation {
            reservation_id,
            amount,
            reason,
        },
    )]
}

fn stable_partition(key: &[u8], partition_count: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    (hash as usize) % partition_count
}

impl From<crate::repository::WalletRepositoryError> for QueueError {
    fn from(error: crate::repository::WalletRepositoryError) -> Self {
        Self::Repository(format!("{error:?}"))
    }
}

impl From<crate::worker::WalletError> for QueueError {
    fn from(error: crate::worker::WalletError) -> Self {
        Self::Worker(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use protocol::{
        common::{Asset, CommandEnvelope, OrderType, Side},
        engine::{
            AccountDelta, EngineCommand, ExecutionReason, FeeCharged, FeeDelta, FundingPayment,
            FundingPaymentApplied, OrderCancelled, OrderExpired, ReservationReleased,
            TradeExecuted, TradeSettlement,
        },
        wallet::{Deposit, PlaceOrderIntent, ReleaseReservation, SettleTrade},
    };

    use crate::engine_inputs::{DEFAULT_ENGINE_INPUT_KEY, EngineInputPublication};

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
        let first = stable_partition(b"user-42", 3);
        let second = stable_partition(b"user-42", 3);

        assert_eq!(first, second);
        assert!(first < 3);
    }

    #[test]
    fn command_metadata_uses_user_key_and_request_reply_key() {
        let command = WalletCommand::Deposit(Deposit {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("deposit-1"),
                user_id: 42,
                reply_partition: 0,
            },
            asset: Asset::USDC,
            amount: 100,
            reference_id: String::from("ref-1"),
        });

        let metadata = CommandMetadata::from_command(&command);

        assert_eq!(metadata.wallet_key, "42");
        assert_eq!(metadata.reply_key, "req-1");
        assert_eq!(metadata.reply_partition, Some(0));
    }

    #[test]
    fn engine_input_key_uses_constant_key_without_input_id() {
        let command = EngineCommand::PlaceOrder(
            PlaceOrderIntent {
                envelope: CommandEnvelope {
                    request_id: String::from("req-1"),
                    idempotency_key: String::from("order-1"),
                    user_id: 42,
                    reply_partition: 0,
                },
                market_id: 7,
                market_name: String::from("SOL-PERP"),
                side: Side::LONG,
                order_type: OrderType::LIMIT,
                quantity: 10,
                price: 20,
                margin_asset: Asset::USDC,
                required_margin: 200,
                leverage: 3,
                reduce_only: false,
            }
            .into_reserved_order(String::from("res-1")),
        );

        let publication = EngineInputPublication::new(command);

        assert_eq!(publication.key(), DEFAULT_ENGINE_INPUT_KEY);
    }

    #[test]
    fn engine_input_key_uses_input_id_when_present() {
        let mut command = PlaceOrderIntent {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("order-1"),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 7,
            market_name: String::from("SOL-PERP"),
            side: Side::LONG,
            order_type: OrderType::LIMIT,
            quantity: 10,
            price: 20,
            margin_asset: Asset::USDC,
            required_margin: 200,
            leverage: 3,
            reduce_only: false,
        }
        .into_reserved_order(String::from("res-1"));
        command.input_id = Some(String::from("input-1"));

        let publication = EngineInputPublication::new(EngineCommand::PlaceOrder(command));

        assert_eq!(publication.key(), "input-1");
    }

    #[test]
    fn engine_cancel_event_releases_reservation() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::OrderCancelled(OrderCancelled {
                engine_event_id: Some(String::from("eng-1")),
                engine_sequence: 1,
                engine_timestamp_ms: 1_710_000_000_000,
                source_input_id: None,
                source_input_offset: None,
                order_id: 99,
                reservation_id: String::from("res-1"),
                user_id: 42,
                market_id: 1,
                released_amount: 50,
            }));

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ReleaseReservation(
                ReleaseReservation {
                    reservation_id: String::from("res-1"),
                    amount: 50,
                    reason: String::from("order_cancelled:99"),
                }
            )]
        );
    }

    #[test]
    fn engine_order_expired_event_releases_positive_amount() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::OrderExpired(OrderExpired {
                engine_event_id: Some(String::from("eng-2")),
                market_id: 1,
                engine_sequence: 2,
                engine_timestamp_ms: 1_710_000_000_001,
                source_input_id: None,
                source_input_offset: None,
                order_id: 100,
                reservation_id: String::from("res-2"),
                user_id: 42,
                expired_quantity: 4,
                released_amount: 25,
                reason: String::from("TTL"),
            }));

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ReleaseReservation(
                ReleaseReservation {
                    reservation_id: String::from("res-2"),
                    amount: 25,
                    reason: String::from("order_expired:100:TTL"),
                }
            )]
        );
    }

    #[test]
    fn engine_order_expired_event_ignores_zero_release() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::OrderExpired(OrderExpired {
                engine_event_id: Some(String::from("eng-3")),
                market_id: 1,
                engine_sequence: 3,
                engine_timestamp_ms: 1_710_000_000_002,
                source_input_id: None,
                source_input_offset: None,
                order_id: 101,
                reservation_id: String::from("res-3"),
                user_id: 42,
                expired_quantity: 4,
                released_amount: 0,
                reason: String::from("REDUCE_ONLY_REMAINDER"),
            }));

        assert!(commands.is_empty());
    }

    #[test]
    fn engine_reservation_released_event_releases_reservation() {
        let commands = engine_wallet_commands_from_engine_event(EngineEvent::ReservationReleased(
            ReservationReleased {
                engine_event_id: Some(String::from("eng-4")),
                market_id: 1,
                engine_sequence: 4,
                engine_timestamp_ms: 1_710_000_000_003,
                source_input_id: None,
                source_input_offset: None,
                reservation_id: String::from("res-4"),
                user_id: 42,
                asset: Asset::USDC,
                released_amount: 12,
                reason: String::from("ORDER_REJECTED_AFTER_RESERVATION"),
            },
        ));

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ReleaseReservation(
                ReleaseReservation {
                    reservation_id: String::from("res-4"),
                    amount: 12,
                    reason: String::from("reservation_released:ORDER_REJECTED_AFTER_RESERVATION"),
                }
            )]
        );
    }

    #[test]
    fn engine_trade_event_becomes_settlement_and_fee_commands() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::TradeExecuted(TradeExecuted {
                engine_event_id: Some(String::from("eng-5")),
                engine_sequence: 5,
                engine_timestamp_ms: 1_710_000_000_004,
                source_input_id: None,
                source_input_offset: None,
                fill_id: 7,
                market_id: 1,
                price: 100,
                quantity: 2,
                maker_order_id: 10,
                taker_order_id: 11,
                maker_user_id: 42,
                taker_user_id: 43,
                maker_reservation_id: Some(String::from("res-maker")),
                taker_reservation_id: Some(String::from("res-taker")),
                execution_reason: ExecutionReason::TRADE,
                liquidation_id: None,
                liquidated_user_id: None,
                position_side: None,
                liquidation_fee: None,
                fee_deltas: vec![FeeDelta {
                    user_id: 43,
                    asset: Asset::USDC,
                    amount: 3,
                    fee_type: String::from("TAKER"),
                }],
                settlements: vec![TradeSettlement {
                    reservation_id: String::from("res-maker"),
                    debit_asset: Asset::USDC,
                    debit_amount: 25,
                    credit_asset: Asset::USDC,
                    credit_amount: 1,
                }],
            }));

        assert_eq!(
            commands,
            vec![
                EngineWalletCommand::SettleTrade(SettleTrade {
                    fill_id: 7,
                    reservation_id: String::from("res-maker"),
                    debit_asset: Asset::USDC,
                    debit_amount: 25,
                    credit_asset: Asset::USDC,
                    credit_amount: 1,
                }),
                EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                    user_id: 43,
                    asset: Asset::USDC,
                    total_delta: -3,
                    locked_delta: 0,
                    kind: String::from("TRADE_FEE"),
                    reference_id: String::from("fill:7:fee:0:43:TAKER"),
                }),
            ]
        );
    }

    #[test]
    fn funding_payment_event_becomes_account_delta_commands() {
        let commands = engine_wallet_commands_from_engine_event(
            EngineEvent::FundingPaymentApplied(FundingPaymentApplied {
                engine_event_id: Some(String::from("eng-6")),
                market_id: 1,
                engine_sequence: 6,
                engine_timestamp_ms: 1_710_000_000_005,
                source_input_id: None,
                source_input_offset: None,
                funding_interval_id: String::from("funding-1"),
                payments: vec![FundingPayment {
                    user_id: 42,
                    position_id: String::from("pos-42-1"),
                    side: Side::LONG,
                    asset: Asset::USDC,
                    amount: -2,
                }],
            }),
        );

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                user_id: 42,
                asset: Asset::USDC,
                total_delta: -2,
                locked_delta: 0,
                kind: String::from("FUNDING_PAYMENT"),
                reference_id: String::from("eng-6:payment:0:pos-42-1"),
            })]
        );
    }

    #[test]
    fn fee_charged_event_becomes_account_delta_command() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::FeeCharged(FeeCharged {
                engine_event_id: Some(String::from("eng-7")),
                market_id: 1,
                engine_sequence: 7,
                engine_timestamp_ms: 1_710_000_000_006,
                source_input_id: None,
                source_input_offset: None,
                fee_id: String::from("fee-1"),
                user_id: 42,
                asset: Asset::USDC,
                amount: 5,
                fee_type: String::from("LIQUIDATION"),
                destination: String::from("INSURANCE_FUND"),
            }));

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                user_id: 42,
                asset: Asset::USDC,
                total_delta: -5,
                locked_delta: 0,
                kind: String::from("FEE_CHARGED"),
                reference_id: String::from("fee-1"),
            })]
        );
    }

    #[test]
    fn account_delta_event_becomes_account_delta_command() {
        let commands =
            engine_wallet_commands_from_engine_event(EngineEvent::AccountDelta(AccountDelta {
                engine_event_id: Some(String::from("eng-8")),
                market_id: 1,
                engine_sequence: 8,
                engine_timestamp_ms: 1_710_000_000_007,
                source_input_id: None,
                source_input_offset: None,
                account_delta_id: String::from("acct-1"),
                user_id: 42,
                asset: Asset::USDC,
                total_delta: -205,
                locked_delta: -100,
                reason: String::from("LIQUIDATION_SETTLEMENT"),
                reference_id: String::from("liq-1"),
            }));

        assert_eq!(
            commands,
            vec![EngineWalletCommand::ApplyAccountDelta(ApplyAccountDelta {
                user_id: 42,
                asset: Asset::USDC,
                total_delta: -205,
                locked_delta: -100,
                kind: String::from("ACCOUNT_DELTA"),
                reference_id: String::from("acct-1"),
            })]
        );
    }
}
