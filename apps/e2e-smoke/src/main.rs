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

const MARKET_ID: i64 = 1;
const MARKET_NAME: &str = "SOL-PERP";

#[derive(Debug, Clone)]
struct Settings {
    server_url: String,
    ws_url: String,
    database_url: String,
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

    wait_for_server(&client, &settings.server_url).await?;
    upsert_market(&pool).await?;

    let run_id = unix_millis();
    let alice = signup(&client, &settings.server_url, &format!("alice-{run_id}")).await?;
    let bob = signup(&client, &settings.server_url, &format!("bob-{run_id}")).await?;

    println!(
        "created smoke users {}={} {}={}",
        alice.username, alice.userid, bob.username, bob.userid
    );

    let mut alice_ws = connect_ws(&settings.ws_url, &alice.jwt_token).await?;
    let mut bob_ws = connect_ws(&settings.ws_url, &bob.jwt_token).await?;
    subscribe_market(&mut alice_ws).await?;
    subscribe_market(&mut bob_ws).await?;

    command(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        &format!("deposit-alice-{run_id}"),
        "/balance/",
        json!({"asset":"USDC","amount":10000,"reference_id":format!("deposit-alice-{run_id}")}),
    )
    .await
    .context("alice deposit failed")?;
    command(
        &client,
        &settings.server_url,
        &bob.jwt_token,
        &format!("deposit-bob-{run_id}"),
        "/balance/",
        json!({"asset":"USDC","amount":10000,"reference_id":format!("deposit-bob-{run_id}")}),
    )
    .await
    .context("bob deposit failed")?;

    command(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        &format!("order-alice-{run_id}"),
        "/orders/",
        json!({
            "market_id": MARKET_ID,
            "market_name": MARKET_NAME,
            "side": "LONG",
            "order_type": "LIMIT",
            "quantity": 10,
            "price": 100,
            "margin": 1000,
            "margin_asset": "USDC"
        }),
    )
    .await
    .context("alice order failed")?;

    wait_for_message(
        &alice_ws.messages,
        "alice market orderbook bid",
        is_market_orderbook_bid,
    )
    .await?;
    wait_for_orderbook_snapshot(&client, &settings.server_url).await?;

    command(
        &client,
        &settings.server_url,
        &bob.jwt_token,
        &format!("order-bob-{run_id}"),
        "/orders/",
        json!({
            "market_id": MARKET_ID,
            "market_name": MARKET_NAME,
            "side": "SHORT",
            "order_type": "LIMIT",
            "quantity": 10,
            "price": 100,
            "margin": 1000,
            "margin_asset": "USDC"
        }),
    )
    .await
    .context("bob order failed")?;

    wait_for_message(&alice_ws.messages, "alice account trade", is_account_trade).await?;
    wait_for_message(&bob_ws.messages, "bob account trade", is_account_trade).await?;
    wait_for_message(&alice_ws.messages, "alice market trade", is_market_trade).await?;
    wait_for_message(&bob_ws.messages, "bob market trade", is_market_trade).await?;
    wait_for_message(
        &alice_ws.messages,
        "alice market orderbook clear",
        is_market_orderbook_clear,
    )
    .await?;
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

    wait_for_projected_fill(&pool, alice.userid, bob.userid).await?;
    wait_for_candles(&pool).await?;
    wait_for_candle_api(&client, &settings.server_url).await?;
    wait_for_filled_orders(&pool, alice.userid, bob.userid).await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        alice.userid,
        "LONG",
    )
    .await?;
    wait_for_open_position_api(
        &client,
        &settings.server_url,
        &bob.jwt_token,
        bob.userid,
        "SHORT",
    )
    .await?;
    wait_for_unlocked_balances(&pool, alice.userid, bob.userid).await?;
    wait_for_ledger_entries(&pool, alice.userid, bob.userid).await?;

    let charlie = signup(&client, &settings.server_url, &format!("charlie-{run_id}")).await?;
    println!(
        "created smoke liquidity user {}={}",
        charlie.username, charlie.userid
    );
    command(
        &client,
        &settings.server_url,
        &charlie.jwt_token,
        &format!("deposit-charlie-{run_id}"),
        "/balance/",
        json!({"asset":"USDC","amount":10000,"reference_id":format!("deposit-charlie-{run_id}")}),
    )
    .await
    .context("charlie deposit failed")?;
    command(
        &client,
        &settings.server_url,
        &charlie.jwt_token,
        &format!("order-charlie-close-liquidity-{run_id}"),
        "/orders/",
        json!({
            "market_id": MARKET_ID,
            "market_name": MARKET_NAME,
            "side": "LONG",
            "order_type": "LIMIT",
            "quantity": 10,
            "price": 100,
            "margin": 1000,
            "margin_asset": "USDC"
        }),
    )
    .await
    .context("charlie close-liquidity order failed")?;
    command(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        &format!("close-alice-{run_id}"),
        "/positions/close",
        json!({
            "market_id": MARKET_ID,
            "price": 0
        }),
    )
    .await
    .context("alice close position failed")?;

    wait_for_no_open_position_api(&client, &settings.server_url, &alice.jwt_token).await?;
    wait_for_closed_position_api(
        &client,
        &settings.server_url,
        &alice.jwt_token,
        alice.userid,
    )
    .await?;
    wait_for_reduce_only_close_order(&pool, alice.userid).await?;

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
        }
    }
}

async fn wait_for_server(client: &Client, server_url: &str) -> Result<()> {
    let url = format!("{server_url}/orders/open/{MARKET_ID}");
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

async fn upsert_market(pool: &Pool<Postgres>) -> Result<()> {
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
        VALUES($1,$2,'SOL','USDC',9,6,0)
        ON CONFLICT(market_id)
        DO UPDATE
        SET market_name=EXCLUDED.market_name,
            base_asset=EXCLUDED.base_asset,
            quote_asset=EXCLUDED.quote_asset,
            decimal_base=EXCLUDED.decimal_base,
            decimal_quote=EXCLUDED.decimal_quote
        "#,
    )
    .bind(MARKET_ID)
    .bind(MARKET_NAME)
    .execute(pool)
    .await
    .context("failed to upsert smoke market")?;

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

async fn subscribe_market(client: &mut WsClient) -> Result<()> {
    client
        .write
        .send(Message::Text(
            json!({"type":"Subscribe","payload":{"markets":[MARKET_ID]}})
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

fn is_market_trade(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("MarketEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("engine")
        && value.pointer("/payload/market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("TradeExecuted")
        && has_engine_sequence(value)
}

fn is_market_orderbook_bid(value: &Value) -> bool {
    is_market_orderbook_delta(value)
        && value
            .pointer("/payload/event/payload/bids/0/price")
            .and_then(Value::as_i64)
            == Some(100)
        && value
            .pointer("/payload/event/payload/bids/0/quantity")
            .and_then(Value::as_i64)
            == Some(10)
}

fn is_market_orderbook_clear(value: &Value) -> bool {
    is_market_orderbook_delta(value)
        && value
            .pointer("/payload/event/payload/bids/0/price")
            .and_then(Value::as_i64)
            == Some(100)
        && value
            .pointer("/payload/event/payload/bids/0/quantity")
            .and_then(Value::as_i64)
            == Some(0)
}

fn is_market_orderbook_delta(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("MarketEvent")
        && value.pointer("/payload/source").and_then(Value::as_str) == Some("engine")
        && value.pointer("/payload/market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value.pointer("/payload/event/type").and_then(Value::as_str) == Some("OrderBookDelta")
        && has_engine_sequence(value)
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

async fn wait_for_projected_fill(pool: &Pool<Postgres>, alice: i64, bob: i64) -> Result<()> {
    wait_for_db("projected fill", || async {
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
        .bind(alice)
        .bind(bob)
        .bind(MARKET_ID)
        .fetch_one(pool)
        .await?;
        Ok(count >= 1)
    })
    .await
}

async fn wait_for_candles(pool: &Pool<Postgres>) -> Result<()> {
    wait_for_db("candles", || async {
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
        .bind(MARKET_ID)
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
                row.get::<i64, _>("open") == 100
                    && row.get::<i64, _>("high") == 100
                    && row.get::<i64, _>("low") == 100
                    && row.get::<i64, _>("close") == 100
                    && row.get::<i64, _>("volume") == 10
                    && row.get::<i64, _>("trade_count") == 1
                    && row.get::<i64, _>("first_engine_sequence") > 0
                    && row.get::<i64, _>("last_engine_sequence")
                        >= row.get::<i64, _>("first_engine_sequence")
            }))
    })
    .await
}

async fn wait_for_candle_api(client: &Client, server_url: &str) -> Result<()> {
    let url = format!("{server_url}/markets/{MARKET_ID}/candles?interval=1m&limit=10");
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
                    .any(is_expected_candle)
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

async fn wait_for_orderbook_snapshot(client: &Client, server_url: &str) -> Result<()> {
    let url = format!("{server_url}/markets/{MARKET_ID}/orderbook?depth=10");
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
                && payload
                    .body
                    .as_ref()
                    .is_some_and(is_expected_orderbook_snapshot)
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

fn is_expected_orderbook_snapshot(value: &Value) -> bool {
    value.get("market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value
            .get("engine_sequence")
            .and_then(Value::as_i64)
            .is_some_and(|sequence| sequence > 0)
        && value.pointer("/bids/0/price").and_then(Value::as_i64) == Some(100)
        && value.pointer("/bids/0/quantity").and_then(Value::as_i64) == Some(10)
}

fn is_expected_candle(value: &Value) -> bool {
    value.get("market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value.get("interval").and_then(Value::as_str) == Some("1m")
        && value.get("open").and_then(Value::as_i64) == Some(100)
        && value.get("high").and_then(Value::as_i64) == Some(100)
        && value.get("low").and_then(Value::as_i64) == Some(100)
        && value.get("close").and_then(Value::as_i64) == Some(100)
        && value.get("volume").and_then(Value::as_i64) == Some(10)
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
    user_id: i64,
    side: &str,
) -> Result<()> {
    let url = format!("{server_url}/positions/open/{MARKET_ID}");
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
                .is_some_and(|position| is_expected_position(position, user_id, side))
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

fn is_expected_position(value: &Value, user_id: i64, side: &str) -> bool {
    value.get("user_id").and_then(Value::as_i64) == Some(user_id)
        && value.get("market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value.get("side").and_then(Value::as_str) == Some(side)
        && value.get("quantity").and_then(Value::as_i64) == Some(10)
        && value.get("average_price").and_then(Value::as_i64) == Some(100)
        && value.get("initial_margin").and_then(Value::as_i64) == Some(1000)
        && value.get("unrealized_pnl").and_then(Value::as_i64) == Some(0)
}

async fn wait_for_no_open_position_api(
    client: &Client,
    server_url: &str,
    token: &str,
) -> Result<()> {
    let url = format!("{server_url}/positions/open/{MARKET_ID}");
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
    user_id: i64,
) -> Result<()> {
    let url = format!("{server_url}/positions/closed/{MARKET_ID}");
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
                    .any(|position| is_expected_closed_position(position, user_id))
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

fn is_expected_closed_position(value: &Value, user_id: i64) -> bool {
    value.get("user_id").and_then(Value::as_i64) == Some(user_id)
        && value.get("market_id").and_then(Value::as_i64) == Some(MARKET_ID)
        && value.get("side").and_then(Value::as_str) == Some("LONG")
        && value.get("quantity").and_then(Value::as_i64) == Some(10)
        && value.get("entry_price").and_then(Value::as_i64) == Some(100)
        && value.get("exit_price").and_then(Value::as_i64) == Some(100)
        && value.get("realized_pnl").and_then(Value::as_i64) == Some(0)
        && value
            .get("close_order_id")
            .and_then(Value::as_i64)
            .is_some_and(|order_id| order_id > 0)
}

async fn wait_for_filled_orders(pool: &Pool<Postgres>, alice: i64, bob: i64) -> Result<()> {
    wait_for_db("filled orders", || async {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM orders
            WHERE user_id IN ($1,$2) AND market_id=$3 AND status='FILLED'
            "#,
        )
        .bind(alice)
        .bind(bob)
        .bind(MARKET_ID)
        .fetch_one(pool)
        .await?;
        Ok(count == 2)
    })
    .await
}

async fn wait_for_reduce_only_close_order(pool: &Pool<Postgres>, user_id: i64) -> Result<()> {
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
        .bind(MARKET_ID)
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
