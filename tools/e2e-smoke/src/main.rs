use std::{collections::BTreeSet, env, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{Pool, Postgres, Row, postgres::PgPoolOptions};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy)]
struct MarketSpec {
    id: i64,
    name: &'static str,
    base_asset: &'static str,
    price: i64,
    quantity: i64,
    margin: i64,
}

const PRIMARY_MARKET: MarketSpec = MarketSpec {
    id: 1,
    name: "SOL-PERP",
    base_asset: "SOL",
    price: 100,
    quantity: 10,
    margin: 1000,
};
const SECONDARY_MARKET: MarketSpec = MarketSpec {
    id: 2,
    name: "ETH-PERP",
    base_asset: "ETH",
    price: 100,
    quantity: 3,
    margin: 300,
};

#[derive(Debug, Clone)]
struct Settings {
    server_url: String,
    ws_url: String,
    database_url: String,
    mark_input_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    info: String,
    body: Option<T>,
}

#[derive(Debug, Deserialize)]
struct UserRecord {
    userid: i64,
    username: String,
    jwt_token: String,
}

struct WsClient {
    write: SplitSink<WsStream, Message>,
    messages: std::sync::Arc<Mutex<Vec<Value>>>,
    _reader: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env();
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build HTTP client")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database_url)
        .await
        .context("failed to connect to smoke database")?;

    wait_for_server(&client, &settings.server_url, PRIMARY_MARKET).await?;
    upsert_market(&pool, PRIMARY_MARKET).await?;
    upsert_market(&pool, SECONDARY_MARKET).await?;

    let run_id = unix_millis();
    let alice = signup(&client, &settings.server_url, &format!("alice-{run_id}")).await?;
    let bob = signup(&client, &settings.server_url, &format!("bob-{run_id}")).await?;
    let dave = signup(&client, &settings.server_url, &format!("dave-{run_id}")).await?;
    let erin = signup(&client, &settings.server_url, &format!("erin-{run_id}")).await?;

    println!(
        "created smoke users {}={} {}={} {}={} {}={}",
        alice.username,
        alice.userid,
        bob.username,
        bob.userid,
        dave.username,
        dave.userid,
        erin.username,
        erin.userid
    );

    let mut alice_ws = connect_ws(&settings.ws_url, &alice.jwt_token).await?;
    let mut bob_ws = connect_ws(&settings.ws_url, &bob.jwt_token).await?;
    let mut dave_ws = connect_ws(&settings.ws_url, &dave.jwt_token).await?;
    let mut erin_ws = connect_ws(&settings.ws_url, &erin.jwt_token).await?;
    subscribe_market(&mut alice_ws, PRIMARY_MARKET).await?;
    subscribe_market(&mut bob_ws, PRIMARY_MARKET).await?;
    subscribe_market(&mut dave_ws, SECONDARY_MARKET).await?;
    subscribe_market(&mut erin_ws, SECONDARY_MARKET).await?;

    for user in [&alice, &bob, &dave, &erin] {
        deposit_usdc(
            &client,
            &settings.server_url,
            user,
            &format!("deposit-{}", user.username),
        )
        .await?;
    }

    place_limit_order(
        &client,
        &settings.server_url,
        &dave,
        &format!("order-dave-secondary-bid-{run_id}"),
        SECONDARY_MARKET,
        "LONG",
    )
    .await?;

    wait_for_message(
        &dave_ws.messages,
        "dave secondary market orderbook bid",
        |value| is_market_orderbook_bid(value, SECONDARY_MARKET),
    )
    .await?;
    wait_for_message(
        &erin_ws.messages,
        "erin secondary market orderbook bid",
        |value| is_market_orderbook_bid(value, SECONDARY_MARKET),
    )
    .await?;
    wait_for_orderbook_snapshot(&client, &settings.server_url, SECONDARY_MARKET).await?;

    place_limit_order(
        &client,
        &settings.server_url,
        &alice,
        &format!("order-alice-primary-bid-{run_id}"),
        PRIMARY_MARKET,
        "LONG",
    )
    .await?;

    wait_for_message(
        &alice_ws.messages,
        "alice primary market orderbook bid",
        |value| is_market_orderbook_bid(value, PRIMARY_MARKET),
    )
    .await?;
    wait_for_message(
        &bob_ws.messages,
        "bob primary market orderbook bid",
        |value| is_market_orderbook_bid(value, PRIMARY_MARKET),
    )
    .await?;
    wait_for_orderbook_snapshot(&client, &settings.server_url, PRIMARY_MARKET).await?;
    wait_for_orderbook_snapshot(&client, &settings.server_url, SECONDARY_MARKET).await?;
    assert_market_events_only(
        &alice_ws.messages,
        "alice primary websocket",
        &[PRIMARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &bob_ws.messages,
        "bob primary websocket",
        &[PRIMARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &dave_ws.messages,
        "dave secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &erin_ws.messages,
        "erin secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;

    place_limit_order(
        &client,
        &settings.server_url,
        &bob,
        &format!("order-bob-primary-fill-{run_id}"),
        PRIMARY_MARKET,
        "SHORT",
    )
    .await?;

    wait_for_message(&alice_ws.messages, "alice account trade", is_account_trade).await?;
    wait_for_message(&bob_ws.messages, "bob account trade", is_account_trade).await?;
    wait_for_message(&alice_ws.messages, "alice primary market trade", |value| {
        is_market_trade(value, PRIMARY_MARKET)
    })
    .await?;
    wait_for_message(&bob_ws.messages, "bob primary market trade", |value| {
        is_market_trade(value, PRIMARY_MARKET)
    })
    .await?;
    wait_for_message(
        &alice_ws.messages,
        "alice primary market orderbook clear",
        |value| is_market_orderbook_clear(value, PRIMARY_MARKET),
    )
    .await?;
    wait_for_empty_orderbook_snapshot(&client, &settings.server_url, PRIMARY_MARKET).await?;
    wait_for_message(
        &alice_ws.messages,
        "alice wallet settlement",
        is_wallet_settlement,
    )
    .await?;
    wait_for_message(
        &bob_ws.messages,
        "bob wallet settlement",
        is_wallet_settlement,
    )
    .await?;

    wait_for_projected_fill(&pool, PRIMARY_MARKET, alice.userid, bob.userid).await?;
    wait_for_candles(&pool, PRIMARY_MARKET).await?;
    wait_for_candle_api(&client, &settings.server_url, PRIMARY_MARKET).await?;
    wait_for_filled_orders(&pool, PRIMARY_MARKET, alice.userid, bob.userid).await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        PRIMARY_MARKET,
        alice.userid,
        "LONG",
    )
    .await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &bob.jwt_token,
        PRIMARY_MARKET,
        bob.userid,
        "SHORT",
    )
    .await?;
    wait_for_unlocked_balances(&pool, alice.userid, bob.userid).await?;
    wait_for_ledger_entries(&pool, alice.userid, bob.userid).await?;

    place_limit_order(
        &client,
        &settings.server_url,
        &erin,
        &format!("order-erin-secondary-fill-{run_id}"),
        SECONDARY_MARKET,
        "SHORT",
    )
    .await?;

    wait_for_message(&dave_ws.messages, "dave account trade", is_account_trade).await?;
    wait_for_message(&erin_ws.messages, "erin account trade", is_account_trade).await?;
    wait_for_message(&dave_ws.messages, "dave secondary market trade", |value| {
        is_market_trade(value, SECONDARY_MARKET)
    })
    .await?;
    wait_for_message(&erin_ws.messages, "erin secondary market trade", |value| {
        is_market_trade(value, SECONDARY_MARKET)
    })
    .await?;
    wait_for_message(
        &dave_ws.messages,
        "dave secondary market orderbook clear",
        |value| is_market_orderbook_clear(value, SECONDARY_MARKET),
    )
    .await?;
    wait_for_empty_orderbook_snapshot(&client, &settings.server_url, SECONDARY_MARKET).await?;
    wait_for_message(
        &dave_ws.messages,
        "dave wallet settlement",
        is_wallet_settlement,
    )
    .await?;
    wait_for_message(
        &erin_ws.messages,
        "erin wallet settlement",
        is_wallet_settlement,
    )
    .await?;

    wait_for_projected_fill(&pool, SECONDARY_MARKET, dave.userid, erin.userid).await?;
    wait_for_candles(&pool, SECONDARY_MARKET).await?;
    wait_for_candle_api(&client, &settings.server_url, SECONDARY_MARKET).await?;
    wait_for_filled_orders(&pool, SECONDARY_MARKET, dave.userid, erin.userid).await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &dave.jwt_token,
        SECONDARY_MARKET,
        dave.userid,
        "LONG",
    )
    .await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &erin.jwt_token,
        SECONDARY_MARKET,
        erin.userid,
        "SHORT",
    )
    .await?;
    wait_for_unlocked_balances(&pool, dave.userid, erin.userid).await?;
    wait_for_ledger_entries(&pool, dave.userid, erin.userid).await?;
    assert_market_events_only(
        &alice_ws.messages,
        "alice primary websocket",
        &[PRIMARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &bob_ws.messages,
        "bob primary websocket",
        &[PRIMARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &dave_ws.messages,
        "dave secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &erin_ws.messages,
        "erin secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;

    let charlie = signup(&client, &settings.server_url, &format!("charlie-{run_id}")).await?;
    println!(
        "created smoke liquidity user {}={}",
        charlie.username, charlie.userid
    );
    deposit_usdc(
        &client,
        &settings.server_url,
        &charlie,
        &format!("deposit-charlie-{run_id}"),
    )
    .await?;
    place_limit_order(
        &client,
        &settings.server_url,
        &charlie,
        &format!("order-charlie-close-liquidity-{run_id}"),
        PRIMARY_MARKET,
        "LONG",
    )
    .await?;
    command(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        &format!("close-alice-{run_id}"),
        "/positions/close",
        json!({
            "market_id": PRIMARY_MARKET.id,
            "price": 0
        }),
    )
    .await
    .context("alice close position failed")?;

    wait_for_no_open_position_api(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        PRIMARY_MARKET,
    )
    .await?;
    wait_for_closed_position_api(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        PRIMARY_MARKET,
        alice.userid,
    )
    .await?;
    wait_for_reduce_only_close_order(&pool, PRIMARY_MARKET, alice.userid).await?;
    assert_market_events_only(
        &dave_ws.messages,
        "dave secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;
    assert_market_events_only(
        &erin_ws.messages,
        "erin secondary websocket",
        &[SECONDARY_MARKET.id],
    )
    .await?;
    if let Some(mark_input_id) = settings.mark_input_id.as_deref() {
        wait_for_mark_ingress_outbox_published(&pool, mark_input_id).await?;
    }
    wait_for_wallet_outbox_drained(&pool).await?;
    wait_for_wallet_ledger_logical_event_ids(&pool).await?;

    println!("e2e smoke passed");
    Ok(())
}

impl Settings {
    fn from_env() -> Self {
        Self {
            server_url: env::var("E2E_SERVER_URL")
                .unwrap_or_else(|_| String::from("http://127.0.0.1:18080/api")),
            ws_url: env::var("E2E_WS_URL")
                .unwrap_or_else(|_| String::from("ws://127.0.0.1:18081/ws")),
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
                String::from("postgres://postgres:postgres@127.0.0.1:55432/exchange")
            }),
            mark_input_id: env::var("E2E_MARK_INPUT_ID").ok(),
        }
    }
}

async fn wait_for_server(client: &Client, server_url: &str, market: MarketSpec) -> Result<()> {
    let url = format!("{server_url}/orders/open/{}", market.id);
    let deadline = Instant::now() + Duration::from_secs(60);

    loop {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                if Instant::now() >= deadline {
                    bail!("server readiness returned {}", response.status());
                }
            }
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error).context("server did not become ready");
                }
            }
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn upsert_market(pool: &Pool<Postgres>, market: MarketSpec) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO markets(
            market_id,
            market_name,
            base_asset,
            quote_asset,
            decimal_base,
            decimal_quote,
            last_traded_price
        )
        VALUES($1,$2,$3::asset_type,'USDC'::asset_type,9,6,0)
        ON CONFLICT(market_id)
        DO UPDATE
        SET market_name=EXCLUDED.market_name,
            base_asset=EXCLUDED.base_asset,
            quote_asset=EXCLUDED.quote_asset,
            decimal_base=EXCLUDED.decimal_base,
            decimal_quote=EXCLUDED.decimal_quote
        "#,
    )
    .bind(market.id)
    .bind(market.name)
    .bind(market.base_asset)
    .execute(pool)
    .await
    .with_context(|| format!("failed to upsert smoke market {}", market.name))?;

    Ok(())
}

async fn signup(client: &Client, server_url: &str, username: &str) -> Result<UserRecord> {
    let response = client
        .post(format!("{server_url}/auth/signup"))
        .json(&json!({"username":username,"password":"password"}))
        .send()
        .await
        .with_context(|| format!("signup request failed for {username}"))?;
    let status = response.status();
    let payload = response
        .json::<ApiResponse<UserRecord>>()
        .await
        .with_context(|| format!("signup response was not valid JSON for {username}"))?;

    if status != StatusCode::CREATED || !payload.success {
        bail!("signup failed for {username}: {status} {}", payload.info);
    }

    payload
        .body
        .ok_or_else(|| anyhow!("signup response missing body for {username}"))
}

async fn deposit_usdc(
    client: &Client,
    server_url: &str,
    user: &UserRecord,
    idempotency_key: &str,
) -> Result<()> {
    command(
        client,
        server_url,
        &user.jwt_token,
        idempotency_key,
        "/balance/",
        json!({"asset":"USDC","amount":10000,"reference_id":idempotency_key}),
    )
    .await
    .with_context(|| format!("{} deposit failed", user.username))?;

    Ok(())
}

async fn place_limit_order(
    client: &Client,
    server_url: &str,
    user: &UserRecord,
    idempotency_key: &str,
    market: MarketSpec,
    side: &str,
) -> Result<()> {
    command(
        client,
        server_url,
        &user.jwt_token,
        idempotency_key,
        "/orders/",
        json!({
            "market_id": market.id,
            "market_name": market.name,
            "side": side,
            "order_type": "LIMIT",
            "quantity": market.quantity,
            "price": market.price,
            "margin": market.margin,
            "margin_asset": "USDC"
        }),
    )
    .await
    .with_context(|| format!("{} {side} order failed on {}", user.username, market.name))?;

    Ok(())
}

async fn command(
    client: &Client,
    server_url: &str,
    token: &str,
    idempotency_key: &str,
    path: &str,
    body: Value,
) -> Result<Value> {
    let response = client
        .post(format!("{server_url}{path}"))
        .bearer_auth(token)
        .header("Idempotency-Key", idempotency_key)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("command request failed for {path}"))?;
    let status = response.status();
    let payload = response
        .json::<ApiResponse<Value>>()
        .await
        .with_context(|| format!("command response was not valid JSON for {path}"))?;

    if !status.is_success() || !payload.success {
        bail!("command failed for {path}: {status} {}", payload.info);
    }

    let body = payload
        .body
        .ok_or_else(|| anyhow!("command response missing body for {path}"))?;

    if status == StatusCode::ACCEPTED {
        let request_id = body
            .get("request_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("queued command missing request_id for {path}"))?;
        return poll_request(client, server_url, token, request_id).await;
    }

    ensure_complete(&body, path)?;
    Ok(body)
}

async fn poll_request(
    client: &Client,
    server_url: &str,
    token: &str,
    request_id: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(format!("{server_url}/requests/{request_id}"))
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("request status failed for {request_id}"))?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Value>>()
                .await
                .with_context(|| format!("request status was not valid JSON for {request_id}"))?;
            if let Some(body) = payload.body {
                if body
                    .get("complete")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    return Ok(body);
                }
            }
        }

        if Instant::now() >= deadline {
            bail!("request {request_id} did not complete");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn ensure_complete(body: &Value, label: &str) -> Result<()> {
    if body
        .get("complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Ok(())
    } else {
        bail!("command {label} was not complete: {body}");
    }
}

async fn connect_ws(ws_url: &str, token: &str) -> Result<WsClient> {
    let url = format!("{ws_url}?token={token}");
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        match connect_async(&url).await {
            Ok((stream, _)) => {
                let (write, mut read) = stream.split();
                let messages = std::sync::Arc::new(Mutex::new(Vec::new()));
                let reader_messages = messages.clone();
                let reader = tokio::spawn(async move {
                    while let Some(message) = read.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                    reader_messages.lock().await.push(value);
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                });

                let client = WsClient {
                    write,
                    messages,
                    _reader: reader,
                };
                wait_for_message(&client.messages, "websocket welcome", |value| {
                    value.get("type").and_then(Value::as_str) == Some("Welcome")
                })
                .await?;
                return Ok(client);
            }
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error).context("websocket did not become ready");
                }
            }
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn subscribe_market(client: &mut WsClient, market: MarketSpec) -> Result<()> {
    client
        .write
        .send(Message::Text(
            json!({"type":"Subscribe","payload":{"markets":[market.id]}})
                .to_string()
                .into(),
        ))
        .await
        .context("failed to send websocket subscription")?;

    wait_for_message(&client.messages, "websocket subscribed", |value| {
        value.get("type").and_then(Value::as_str) == Some("Subscribed")
    })
    .await?;

    Ok(())
}

async fn wait_for_message<F>(
    messages: &std::sync::Arc<Mutex<Vec<Value>>>,
    label: &str,
    predicate: F,
) -> Result<Value>
where
    F: Fn(&Value) -> bool,
{
    timeout(Duration::from_secs(30), async {
        loop {
            {
                let messages = messages.lock().await;
                if let Some(message) = messages.iter().find(|message| predicate(message)).cloned() {
                    return message;
                }
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for {label}"))
}

fn is_account_trade(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("AccountEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("engine")
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("TradeExecuted")
        && has_engine_sequence(value)
}

fn is_market_trade(value: &Value, market: MarketSpec) -> bool {
    value.get("type").and_then(Value::as_str) == Some("MarketEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("engine")
        && value.pointer("/payload/market_id").and_then(Value::as_i64) == Some(market.id)
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("TradeExecuted")
        && has_engine_sequence(value)
}

fn is_market_orderbook_bid(value: &Value, market: MarketSpec) -> bool {
    is_market_orderbook_delta(value, market)
        && value
            .pointer("/payload/event/payload/bids/0/price")
            .and_then(Value::as_i64)
            == Some(market.price)
        && value
            .pointer("/payload/event/payload/bids/0/quantity")
            .and_then(Value::as_i64)
            == Some(market.quantity)
}

fn is_market_orderbook_clear(value: &Value, market: MarketSpec) -> bool {
    is_market_orderbook_delta(value, market)
        && value
            .pointer("/payload/event/payload/bids/0/price")
            .and_then(Value::as_i64)
            == Some(market.price)
        && value
            .pointer("/payload/event/payload/bids/0/quantity")
            .and_then(Value::as_i64)
            == Some(0)
}

fn is_market_orderbook_delta(value: &Value, market: MarketSpec) -> bool {
    value.get("type").and_then(Value::as_str) == Some("MarketEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("engine")
        && value.pointer("/payload/market_id").and_then(Value::as_i64) == Some(market.id)
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("OrderBookDelta")
        && has_engine_sequence(value)
}

async fn assert_market_events_only(
    messages: &std::sync::Arc<Mutex<Vec<Value>>>,
    label: &str,
    allowed_market_ids: &[i64],
) -> Result<()> {
    sleep(Duration::from_millis(500)).await;

    let allowed_market_ids = allowed_market_ids.iter().copied().collect::<BTreeSet<_>>();
    let messages = messages.lock().await;
    if let Some((market_id, message)) = messages.iter().find_map(|message| {
        market_event_market_id(message).and_then(|market_id| {
            (!allowed_market_ids.contains(&market_id)).then_some((market_id, message))
        })
    }) {
        bail!("{label} received market event for unexpected market {market_id}: {message}");
    }

    Ok(())
}

fn market_event_market_id(value: &Value) -> Option<i64> {
    (value.get("type").and_then(Value::as_str) == Some("MarketEvent"))
        .then(|| value.pointer("/payload/market_id").and_then(Value::as_i64))
        .flatten()
}

fn has_engine_sequence(value: &Value) -> bool {
    value
        .pointer("/payload/event/payload/engine_sequence")
        .and_then(Value::as_i64)
        .is_some_and(|sequence| sequence > 0)
        && value
            .pointer("/payload/event/payload/engine_timestamp_ms")
            .and_then(Value::as_i64)
            .is_some_and(|timestamp| timestamp > 0)
}

fn is_wallet_settlement(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("AccountEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("wallet")
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("TradeSettled")
}

async fn wait_for_projected_fill(
    pool: &Pool<Postgres>,
    market: MarketSpec,
    maker: i64,
    taker: i64,
) -> Result<()> {
    wait_for_db(&format!("projected fill on {}", market.name), || async {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM fills
            WHERE maker_id=$1 AND taker_id=$2
              AND market_id=$3
              AND engine_sequence > 0
              AND executed_at IS NOT NULL
            "#,
        )
        .bind(maker)
        .bind(taker)
        .bind(market.id)
        .fetch_one(pool)
        .await?;
        Ok(count >= 1)
    })
    .await
}

async fn wait_for_candles(pool: &Pool<Postgres>, market: MarketSpec) -> Result<()> {
    wait_for_db(&format!("candles on {}", market.name), || async {
        let rows = sqlx::query(
            r#"
            SELECT
              interval,
              open,
              high,
              low,
              close,
              volume,
              trade_count,
              first_engine_sequence,
              last_engine_sequence
            FROM candles
            WHERE market_id=$1
            "#,
        )
        .bind(market.id)
        .fetch_all(pool)
        .await?;

        let intervals = rows
            .iter()
            .map(|row| row.get::<String, _>("interval"))
            .collect::<BTreeSet<_>>();
        let expected = ["1m", "5m", "15m", "1h", "1d"]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>();

        Ok(rows.len() == expected.len()
            && intervals == expected
            && rows.iter().all(|row| {
                row.get::<i64, _>("open") == market.price
                    && row.get::<i64, _>("high") == market.price
                    && row.get::<i64, _>("low") == market.price
                    && row.get::<i64, _>("close") == market.price
                    && row.get::<i64, _>("volume") == market.quantity
                    && row.get::<i64, _>("trade_count") == 1
                    && row.get::<i64, _>("first_engine_sequence") > 0
                    && row.get::<i64, _>("last_engine_sequence")
                        >= row.get::<i64, _>("first_engine_sequence")
            }))
    })
    .await
}

async fn wait_for_candle_api(client: &Client, server_url: &str, market: MarketSpec) -> Result<()> {
    let url = format!(
        "{server_url}/markets/{}/candles?interval=1m&limit=10",
        market.id
    );
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(&url)
            .send()
            .await
            .context("candle api request failed")?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Vec<Value>>>()
                .await
                .context("candle api response was not valid JSON")?;
            if payload.success
                && payload
                    .body
                    .unwrap_or_default()
                    .iter()
                    .any(|value| is_expected_candle(value, market))
            {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for candle api");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_orderbook_snapshot(
    client: &Client,
    server_url: &str,
    market: MarketSpec,
) -> Result<()> {
    wait_for_orderbook_snapshot_matching(client, server_url, market, |value| {
        value.pointer("/bids/0/price").and_then(Value::as_i64) == Some(market.price)
            && value.pointer("/bids/0/quantity").and_then(Value::as_i64) == Some(market.quantity)
    })
    .await
}

async fn wait_for_empty_orderbook_snapshot(
    client: &Client,
    server_url: &str,
    market: MarketSpec,
) -> Result<()> {
    wait_for_orderbook_snapshot_matching(client, server_url, market, |value| {
        value
            .get("bids")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
            && value
                .get("asks")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty)
    })
    .await
}

async fn wait_for_orderbook_snapshot_matching<F>(
    client: &Client,
    server_url: &str,
    market: MarketSpec,
    predicate: F,
) -> Result<()>
where
    F: Fn(&Value) -> bool,
{
    let url = format!("{server_url}/markets/{}/orderbook?depth=10", market.id);
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(&url)
            .send()
            .await
            .context("orderbook api request failed")?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Value>>()
                .await
                .context("orderbook api response was not valid JSON")?;
            if payload.success
                && payload.body.as_ref().is_some_and(|value| {
                    is_expected_orderbook_snapshot(value, market) && predicate(value)
                })
            {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for orderbook api");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn is_expected_orderbook_snapshot(value: &Value, market: MarketSpec) -> bool {
    value.get("market_id").and_then(Value::as_i64) == Some(market.id)
        && value
            .get("engine_sequence")
            .and_then(Value::as_i64)
            .is_some_and(|sequence| sequence > 0)
}

fn is_expected_candle(value: &Value, market: MarketSpec) -> bool {
    value.get("market_id").and_then(Value::as_i64) == Some(market.id)
        && value.get("interval").and_then(Value::as_str) == Some("1m")
        && value.get("open").and_then(Value::as_i64) == Some(market.price)
        && value.get("high").and_then(Value::as_i64) == Some(market.price)
        && value.get("low").and_then(Value::as_i64) == Some(market.price)
        && value.get("close").and_then(Value::as_i64) == Some(market.price)
        && value.get("volume").and_then(Value::as_i64) == Some(market.quantity)
        && value.get("trade_count").and_then(Value::as_i64) == Some(1)
        && value
            .get("first_engine_sequence")
            .and_then(Value::as_i64)
            .is_some_and(|sequence| sequence > 0)
}

async fn wait_for_open_position_api(
    client: &Client,
    server_url: &str,
    token: &str,
    market: MarketSpec,
    user_id: i64,
    side: &str,
) -> Result<()> {
    let url = format!("{server_url}/positions/open/{}", market.id);
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("open position api request failed")?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Value>>()
                .await
                .context("open position api response was not valid JSON")?;
            if payload
                .body
                .as_ref()
                .is_some_and(|position| is_expected_position(position, market, user_id, side))
            {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for {side} open position");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn is_expected_position(value: &Value, market: MarketSpec, user_id: i64, side: &str) -> bool {
    value.get("user_id").and_then(Value::as_i64) == Some(user_id)
        && value.get("market_id").and_then(Value::as_i64) == Some(market.id)
        && value.get("side").and_then(Value::as_str) == Some(side)
        && value.get("quantity").and_then(Value::as_i64) == Some(market.quantity)
        && value.get("average_price").and_then(Value::as_i64) == Some(market.price)
        && value.get("initial_margin").and_then(Value::as_i64) == Some(market.margin)
        && value.get("unrealized_pnl").and_then(Value::as_i64) == Some(0)
}

async fn wait_for_no_open_position_api(
    client: &Client,
    server_url: &str,
    token: &str,
    market: MarketSpec,
) -> Result<()> {
    let url = format!("{server_url}/positions/open/{}", market.id);
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("open position api request failed")?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Value>>()
                .await
                .context("open position api response was not valid JSON")?;
            if payload.success && payload.body.is_none() {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for closed open position");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_closed_position_api(
    client: &Client,
    server_url: &str,
    token: &str,
    market: MarketSpec,
    user_id: i64,
) -> Result<()> {
    let url = format!("{server_url}/positions/closed/{}", market.id);
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("closed position api request failed")?;

        if response.status().is_success() {
            let payload = response
                .json::<ApiResponse<Vec<Value>>>()
                .await
                .context("closed position api response was not valid JSON")?;
            if payload.success
                && payload
                    .body
                    .unwrap_or_default()
                    .iter()
                    .any(|position| is_expected_closed_position(position, market, user_id))
            {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for closed position");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn is_expected_closed_position(value: &Value, market: MarketSpec, user_id: i64) -> bool {
    value.get("user_id").and_then(Value::as_i64) == Some(user_id)
        && value.get("market_id").and_then(Value::as_i64) == Some(market.id)
        && value.get("side").and_then(Value::as_str) == Some("LONG")
        && value.get("quantity").and_then(Value::as_i64) == Some(market.quantity)
        && value.get("entry_price").and_then(Value::as_i64) == Some(market.price)
        && value.get("exit_price").and_then(Value::as_i64) == Some(market.price)
        && value.get("realized_pnl").and_then(Value::as_i64) == Some(0)
        && value
            .get("close_order_id")
            .and_then(Value::as_i64)
            .is_some_and(|order_id| order_id > 0)
}

async fn wait_for_filled_orders(
    pool: &Pool<Postgres>,
    market: MarketSpec,
    maker: i64,
    taker: i64,
) -> Result<()> {
    wait_for_db(&format!("filled orders on {}", market.name), || async {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM orders
            WHERE user_id IN ($1,$2) AND market_id=$3 AND status='FILLED'
            "#,
        )
        .bind(maker)
        .bind(taker)
        .bind(market.id)
        .fetch_one(pool)
        .await?;
        Ok(count == 2)
    })
    .await
}

async fn wait_for_reduce_only_close_order(
    pool: &Pool<Postgres>,
    market: MarketSpec,
    user_id: i64,
) -> Result<()> {
    wait_for_db("reduce-only close order", || async {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM orders
            WHERE user_id=$1
              AND market_id=$2
              AND side='SHORT'
              AND quantity=10
              AND status='FILLED'
              AND reduce_only=true
            "#,
        )
        .bind(user_id)
        .bind(market.id)
        .fetch_one(pool)
        .await?;
        Ok(count >= 1)
    })
    .await
}

async fn wait_for_unlocked_balances(pool: &Pool<Postgres>, alice: i64, bob: i64) -> Result<()> {
    wait_for_db("unlocked balances", || async {
        let rows = sqlx::query(
            r#"
            SELECT user_id, locked
            FROM user_collaterals
            WHERE user_id IN ($1,$2) AND asset='USDC'
            "#,
        )
        .bind(alice)
        .bind(bob)
        .fetch_all(pool)
        .await?;

        Ok(rows.len() == 2 && rows.iter().all(|row| row.get::<i64, _>("locked") == 0))
    })
    .await
}

async fn wait_for_ledger_entries(pool: &Pool<Postgres>, alice: i64, bob: i64) -> Result<()> {
    wait_for_db("ledger entries", || async {
        let rows = sqlx::query(
            r#"
            SELECT kind, COUNT(*)::BIGINT AS count
            FROM ledger_entries
            WHERE user_id IN ($1,$2)
            GROUP BY kind
            "#,
        )
        .bind(alice)
        .bind(bob)
        .fetch_all(pool)
        .await?;

        let count_for = |kind: &str| -> i64 {
            rows.iter()
                .find(|row| row.get::<String, _>("kind") == kind)
                .map(|row| row.get::<i64, _>("count"))
                .unwrap_or(0)
        };

        Ok(count_for("DEPOSIT") == 2
            && count_for("RESERVE") == 2
            && count_for("TRADE_DEBIT") == 2
            && count_for("TRADE_CREDIT") == 2)
    })
    .await
}

async fn wait_for_wallet_outbox_drained(pool: &Pool<Postgres>) -> Result<()> {
    wait_for_db("wallet outbox drained", || async {
        let row = sqlx::query(
            r#"
            SELECT
              COUNT(*) FILTER (WHERE status <> 'PUBLISHED')::BIGINT AS unpublished,
              COUNT(*) FILTER (WHERE topic='wallet.events' AND status='PUBLISHED')::BIGINT
                AS published_wallet_events,
              COUNT(*) FILTER (WHERE topic='engine.input' AND status='PUBLISHED')::BIGINT
                AS published_engine_inputs
            FROM wallet_outbox
            "#,
        )
        .fetch_one(pool)
        .await?;

        Ok(row.get::<i64, _>("unpublished") == 0
            && row.get::<i64, _>("published_wallet_events") > 0
            && row.get::<i64, _>("published_engine_inputs") > 0)
    })
    .await
}

async fn wait_for_mark_ingress_outbox_published(
    pool: &Pool<Postgres>,
    input_id: &str,
) -> Result<()> {
    let dedupe_key = format!("engine-input:mark-price:{input_id}");

    wait_for_db("mark ingress outbox publication", || async {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM wallet_outbox
            WHERE dedupe_key=$1
              AND topic='engine.input'
              AND payload_type='EngineCommand'
              AND payload->>'type'='MarkPriceUpdated'
              AND payload #>> '{payload,input_id}' = $2
              AND status='PUBLISHED'
            "#,
        )
        .bind(&dedupe_key)
        .bind(input_id)
        .fetch_one(pool)
        .await?;

        Ok(count == 1)
    })
    .await
}

async fn wait_for_wallet_ledger_logical_event_ids(pool: &Pool<Postgres>) -> Result<()> {
    wait_for_db("wallet ledger logical event ids", || async {
        let row = sqlx::query(
            r#"
            WITH outbox AS (
              SELECT COUNT(*)::BIGINT AS published_wallet_events
              FROM wallet_outbox
              WHERE topic='wallet.events' AND status='PUBLISHED'
            )
            SELECT
              COUNT(ledger_events.event_id)::BIGINT AS ledger_wallet_events,
              COUNT(ledger_events.logical_event_id)::BIGINT AS ledger_wallet_events_with_id,
              COUNT(DISTINCT ledger_events.logical_event_id)::BIGINT
                AS distinct_ledger_wallet_event_ids,
              outbox.published_wallet_events
            FROM outbox
            LEFT JOIN ledger_events ON ledger_events.topic='wallet.events'
            GROUP BY outbox.published_wallet_events
            "#,
        )
        .fetch_one(pool)
        .await?;

        let published_wallet_events = row.get::<i64, _>("published_wallet_events");
        let ledger_wallet_events = row.get::<i64, _>("ledger_wallet_events");
        let ledger_wallet_events_with_id = row.get::<i64, _>("ledger_wallet_events_with_id");
        let distinct_ledger_wallet_event_ids =
            row.get::<i64, _>("distinct_ledger_wallet_event_ids");

        Ok(published_wallet_events > 0
            && ledger_wallet_events == published_wallet_events
            && ledger_wallet_events_with_id == ledger_wallet_events
            && distinct_ledger_wallet_event_ids == ledger_wallet_events)
    })
    .await
}

async fn wait_for_db<F, Fut>(label: &str, mut check: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, sqlx::Error>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        if check()
            .await
            .with_context(|| format!("database check failed for {label}"))?
        {
            return Ok(());
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for {label}");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis()
}
