use std::{
    collections::{BTreeMap, HashMap, hash_map::DefaultHasher},
    env,
    hash::{Hash, Hasher},
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use protocol::engine::{
    CancelAccepted, EngineCommand, EngineEvent, EngineInput, EngineReply, OrderAccepted,
    OrderCancelled, OrderOpened,
};
use rskafka::{
    client::{
        Client, ClientBuilder,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
        producer::{BatchProducer, BatchProducerBuilder, aggregator::RecordAggregator},
    },
    record::Record,
    topic::Topic,
};
use serde::Serialize;
use tokio::{sync::Mutex, task::JoinSet};

const FETCH_BYTES: Range<i32> = 1..52_428_800;
const FETCH_MAX_WAIT_MS: i32 = 100;
const PRODUCER_BATCH_BYTES: usize = 1024 * 1024;
const IDLE_SLEEP: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
struct Settings {
    brokers: String,
    engine_input_topic: String,
    engine_replies_topic: String,
    engine_events_topic: String,
    start_at_latest: bool,
}

#[derive(Clone)]
struct Publishers {
    replies: TopicProducer,
    events: TopicProducer,
}

#[derive(Clone)]
struct TopicProducer {
    topic: String,
    producers: Vec<(i32, Arc<BatchProducer<RecordAggregator>>)>,
}

#[derive(Default)]
struct EnginePeerState {
    sequence: AtomicI64,
    orders: Mutex<HashMap<i64, OrderState>>,
}

#[derive(Debug, Clone)]
struct OrderState {
    reservation_id: String,
    user_id: i64,
    market_id: i64,
    released_amount: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_args()?;
    let brokers = parse_brokers(&settings.brokers)?;
    let client = ClientBuilder::new(brokers)
        .client_id("exchange-bench-engine-peer")
        .build()
        .await
        .context("failed to connect to Redpanda")?;
    let topics = client.list_topics().await?;
    let input_partitions =
        partition_clients(&client, &topics, &settings.engine_input_topic).await?;
    let publishers = Arc::new(Publishers {
        replies: TopicProducer::new(&client, &topics, settings.engine_replies_topic.clone())
            .await?,
        events: TopicProducer::new(&client, &topics, settings.engine_events_topic.clone()).await?,
    });
    let state = Arc::new(EnginePeerState::default());

    eprintln!(
        "exchange benchmark engine peer consuming '{}' and publishing '{}' plus '{}'",
        settings.engine_input_topic, settings.engine_replies_topic, settings.engine_events_topic
    );

    let mut tasks = JoinSet::new();
    for partition_client in input_partitions {
        let settings = settings.clone();
        let publishers = publishers.clone();
        let state = state.clone();
        tasks.spawn(async move {
            consume_partition(settings, partition_client, publishers, state).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

impl Settings {
    fn from_args() -> Result<Self> {
        let mut settings = Self {
            brokers: env::var("EXCHANGE_BENCH_REDPANDA_BROKERS")
                .or_else(|_| env::var("REDPANDA_BROKERS"))
                .unwrap_or_else(|_| String::from("127.0.0.1:19092")),
            engine_input_topic: env::var("ENGINE_INPUT_TOPIC")
                .unwrap_or_else(|_| String::from("engine.input")),
            engine_replies_topic: env::var("ENGINE_REPLIES_TOPIC")
                .unwrap_or_else(|_| String::from("engine.replies")),
            engine_events_topic: env::var("ENGINE_EVENTS_TOPIC")
                .unwrap_or_else(|_| String::from("engine.events")),
            start_at_latest: env_bool("EXCHANGE_BENCH_ENGINE_PEER_START_AT_LATEST", true),
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--brokers" => settings.brokers = next_arg(&mut args, &arg)?,
                "--engine-input-topic" => settings.engine_input_topic = next_arg(&mut args, &arg)?,
                "--engine-replies-topic" => {
                    settings.engine_replies_topic = next_arg(&mut args, &arg)?
                }
                "--engine-events-topic" => {
                    settings.engine_events_topic = next_arg(&mut args, &arg)?
                }
                "--from-earliest" => settings.start_at_latest = false,
                "-h" | "--help" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => bail!("unknown argument: {arg}"),
            }
        }

        Ok(settings)
    }
}

fn print_usage() {
    eprintln!(
        "Usage: exchange-bench-engine-peer [--brokers HOST:PORT] [--engine-input-topic TOPIC] [--engine-replies-topic TOPIC] [--engine-events-topic TOPIC] [--from-earliest]"
    );
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{name} requires a value"))
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"),
        Err(_) => default,
    }
}

async fn consume_partition(
    settings: Settings,
    partition_client: Arc<PartitionClient>,
    publishers: Arc<Publishers>,
    state: Arc<EnginePeerState>,
) -> Result<()> {
    let partition = partition_client.partition();
    let mut next_offset = partition_client
        .get_offset(if settings.start_at_latest {
            OffsetAt::Latest
        } else {
            OffsetAt::Earliest
        })
        .await?;

    eprintln!(
        "exchange benchmark engine peer consuming '{}' partition {} from offset {}",
        settings.engine_input_topic, partition, next_offset
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
            next_offset = record.offset + 1;
            let Some(payload) = record.record.value else {
                continue;
            };

            let input = match serde_json::from_slice::<EngineInput>(&payload) {
                Ok(input) => input,
                Err(error) => {
                    eprintln!(
                        "exchange benchmark engine peer ignored invalid input at {}[{}]@{}: {}",
                        settings.engine_input_topic, partition, record.offset, error
                    );
                    continue;
                }
            };

            handle_input(input, record.offset, &publishers, &state).await?;
        }
    }
}

async fn handle_input(
    input: EngineInput,
    source_input_offset: i64,
    publishers: &Publishers,
    state: &EnginePeerState,
) -> Result<()> {
    match input {
        EngineCommand::PlaceOrder(order) => {
            let source_input_id = order.input_id.clone();
            let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            state.orders.lock().await.insert(
                order.order_id,
                OrderState {
                    reservation_id: order.reservation_id.clone(),
                    user_id: order.envelope.user_id,
                    market_id: order.market_id,
                    released_amount: order.reserved_margin_amount,
                },
            );

            let reply = EngineReply::OrderAccepted(OrderAccepted {
                request_id: order.envelope.request_id.clone(),
                source_input_id: source_input_id.clone(),
                source_input_offset: Some(source_input_offset),
                order_id: order.order_id,
                reservation_id: order.reservation_id.clone(),
            });
            let event = EngineEvent::OrderOpened(OrderOpened {
                engine_event_id: Some(format!(
                    "bench-engine-event:order-opened:{}:{}",
                    order.market_id, sequence
                )),
                engine_sequence: sequence,
                engine_timestamp_ms: unix_millis(),
                source_input_id,
                source_input_offset: Some(source_input_offset),
                order_id: order.order_id,
                reservation_id: order.reservation_id,
                user_id: order.envelope.user_id,
                market_id: order.market_id,
            });

            publishers
                .events
                .publish_json(&order.market_id.to_string(), &event)
                .await?;
            publishers
                .replies
                .publish_json_to_partition(
                    order.envelope.reply_partition,
                    &order.envelope.request_id,
                    &reply,
                )
                .await?;
        }
        EngineCommand::CancelOrder(cancel) => {
            let source_input_id = cancel.input_id.clone();
            let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let order_state = state.orders.lock().await.remove(&cancel.order_id);
            let order_state = order_state.unwrap_or_else(|| OrderState {
                reservation_id: format!("bench-missing-reservation:{}", cancel.order_id),
                user_id: cancel.envelope.user_id,
                market_id: cancel.market_id,
                released_amount: 0,
            });

            let reply = EngineReply::CancelAccepted(CancelAccepted {
                request_id: cancel.envelope.request_id.clone(),
                source_input_id: source_input_id.clone(),
                source_input_offset: Some(source_input_offset),
                order_id: cancel.order_id,
            });
            let event = EngineEvent::OrderCancelled(OrderCancelled {
                engine_event_id: Some(format!(
                    "bench-engine-event:order-cancelled:{}:{}",
                    order_state.market_id, sequence
                )),
                engine_sequence: sequence,
                engine_timestamp_ms: unix_millis(),
                source_input_id,
                source_input_offset: Some(source_input_offset),
                order_id: cancel.order_id,
                reservation_id: order_state.reservation_id,
                user_id: order_state.user_id,
                market_id: order_state.market_id,
                released_amount: order_state.released_amount,
            });

            publishers
                .events
                .publish_json(&order_state.market_id.to_string(), &event)
                .await?;
            publishers
                .replies
                .publish_json_to_partition(
                    cancel.envelope.reply_partition,
                    &cancel.envelope.request_id,
                    &reply,
                )
                .await?;
        }
        EngineCommand::LiquidatePosition(_)
        | EngineCommand::MarkPriceUpdated(_)
        | EngineCommand::FundingRateUpdated(_)
        | EngineCommand::FundingSettlementTick(_) => {}
    }

    Ok(())
}

impl TopicProducer {
    async fn new(client: &Client, topics: &[Topic], topic: String) -> Result<Self> {
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

    async fn publish_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let (partition, producer) = self.producer_for_key(key)?;
        self.publish(producer, key, value)
            .await
            .with_context(|| format!("failed to publish to {}[{partition}]", self.topic))?;
        Ok(())
    }

    async fn publish_json_to_partition<T: Serialize>(
        &self,
        partition: i32,
        key: &str,
        value: &T,
    ) -> Result<()> {
        let producer = self.producer_for_partition(partition)?;
        self.publish(producer, key, value)
            .await
            .with_context(|| format!("failed to publish to {}[{partition}]", self.topic))?;
        Ok(())
    }

    async fn publish<T: Serialize>(
        &self,
        producer: &BatchProducer<RecordAggregator>,
        key: &str,
        value: &T,
    ) -> Result<i64> {
        let payload = serde_json::to_vec(value)?;
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        Ok(producer.produce(record).await?)
    }

    fn producer_for_key(&self, key: &str) -> Result<(i32, &BatchProducer<RecordAggregator>)> {
        if self.producers.is_empty() {
            bail!("topic '{}' has no producers", self.topic);
        }
        let index = stable_partition(key.as_bytes(), self.producers.len());
        let (partition, producer) = &self.producers[index];
        Ok((*partition, producer.as_ref()))
    }

    fn producer_for_partition(&self, partition: i32) -> Result<&BatchProducer<RecordAggregator>> {
        self.producers
            .iter()
            .find(|(candidate, _)| *candidate == partition)
            .map(|(_, producer)| producer.as_ref())
            .ok_or_else(|| anyhow!("topic '{}' has no partition {}", self.topic, partition))
    }
}

async fn partition_clients(
    client: &Client,
    topics: &[Topic],
    topic: &str,
) -> Result<Vec<Arc<PartitionClient>>> {
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

fn topic_partitions(topics: &[Topic], topic: &str) -> Result<Vec<i32>> {
    let partitions = topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .map(|topic| topic.partitions.iter().copied().collect::<Vec<_>>())
        .ok_or_else(|| anyhow!("Redpanda topic '{topic}' was not found"))?;

    if partitions.is_empty() {
        bail!("Redpanda topic '{topic}' has no partitions");
    }

    Ok(partitions)
}

fn parse_brokers(brokers: &str) -> Result<Vec<String>> {
    let brokers = brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(String::from)
        .collect::<Vec<_>>();

    if brokers.is_empty() {
        bail!("Redpanda broker list is empty");
    }

    Ok(brokers)
}

fn stable_partition(key: &[u8], partition_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partition_count
}

fn unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(i64::MAX as u128) as i64
}
