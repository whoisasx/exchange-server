use std::{
    env,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{sync::Semaphore, task::JoinSet, time::sleep};

#[derive(Debug, Clone)]
struct Settings {
    server_url: String,
    commands: usize,
    warmup: usize,
    concurrency: usize,
    request_timeout: Duration,
    run_id: String,
}

#[derive(Debug, Clone, Copy)]
struct MarketSpec {
    id: i64,
    name: &'static str,
    price: i64,
    quantity: i64,
    margin: i64,
}

const BENCH_MARKET: MarketSpec = MarketSpec {
    id: 1,
    name: "SOL-PERP",
    price: 100,
    quantity: 1,
    margin: 100,
};

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    info: String,
    body: Option<T>,
}

#[derive(Debug, Clone, Deserialize)]
struct UserRecord {
    username: String,
    userid: i64,
    jwt_token: String,
}

#[derive(Debug, Serialize)]
struct BenchReport {
    component: &'static str,
    scenario: &'static str,
    transport: &'static str,
    commands: usize,
    warmup: usize,
    concurrency: usize,
    duration_ms: f64,
    throughput_per_sec: f64,
    latency_ns: LatencySummary<u64>,
    latency_ms: LatencySummary<f64>,
}

#[derive(Debug, Serialize)]
struct LatencySummary<T> {
    p50: T,
    p90: T,
    p95: T,
    p99: T,
    p999: T,
    max: T,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_args()?;
    let client = Client::builder()
        .pool_max_idle_per_host(settings.concurrency.max(1))
        .build()
        .context("failed to build HTTP client")?;

    let user = signup(&client, &settings).await?;
    let required_balance = ((settings.commands + settings.warmup + settings.concurrency + 10)
        as i64)
        * BENCH_MARKET.margin;
    deposit_usdc(&client, &settings, &user, required_balance).await?;

    if settings.warmup > 0 {
        run_orders(&client, &settings, &user, 0, settings.warmup).await?;
    }

    let started = Instant::now();
    let latencies = run_orders(
        &client,
        &settings,
        &user,
        settings.warmup,
        settings.commands,
    )
    .await?;
    let duration = started.elapsed();

    let report = build_report(&settings, &latencies, duration);
    println!("{}", serde_json::to_string_pretty(&report)?);

    Ok(())
}

impl Settings {
    fn from_args() -> Result<Self> {
        let mut settings = Self {
            server_url: env::var("EXCHANGE_BENCH_SERVER_URL")
                .unwrap_or_else(|_| String::from("http://127.0.0.1:18080/api")),
            commands: env_usize("EXCHANGE_BENCH_COMMANDS", 10_000)?,
            warmup: env_usize("EXCHANGE_BENCH_WARMUP", 1_000)?,
            concurrency: env_usize("EXCHANGE_BENCH_CONCURRENCY", 1)?,
            request_timeout: Duration::from_millis(env_u64(
                "EXCHANGE_BENCH_REQUEST_TIMEOUT_MS",
                30_000,
            )?),
            run_id: env::var("EXCHANGE_BENCH_RUN_ID").unwrap_or_else(|_| default_run_id()),
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--server-url" => settings.server_url = next_arg(&mut args, &arg)?,
                "--commands" => settings.commands = next_arg(&mut args, &arg)?.parse()?,
                "--warmup" => settings.warmup = next_arg(&mut args, &arg)?.parse()?,
                "--concurrency" => settings.concurrency = next_arg(&mut args, &arg)?.parse()?,
                "--request-timeout-ms" => {
                    let timeout_ms = next_arg(&mut args, &arg)?.parse()?;
                    settings.request_timeout = Duration::from_millis(timeout_ms);
                }
                "--run-id" => settings.run_id = next_arg(&mut args, &arg)?,
                "-h" | "--help" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => bail!("unknown argument: {arg}"),
            }
        }

        if settings.commands == 0 {
            bail!("--commands must be greater than zero");
        }
        if settings.concurrency == 0 {
            bail!("--concurrency must be greater than zero");
        }

        settings.server_url = settings.server_url.trim_end_matches('/').to_string();
        Ok(settings)
    }
}

fn print_usage() {
    eprintln!(
        "Usage: exchange-bench-driver [--server-url URL] [--commands N] [--warmup N] [--concurrency N] [--request-timeout-ms N] [--run-id ID]"
    );
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{name} requires a value"))
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be a positive integer")),
        Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be a positive integer")),
        Err(_) => Ok(default),
    }
}

fn default_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("bench-{}-{}", now.as_secs(), now.subsec_nanos())
}

async fn signup(client: &Client, settings: &Settings) -> Result<UserRecord> {
    let username = format!("bench-user-{}", settings.run_id);
    let response = client
        .post(format!("{}/auth/signup", settings.server_url))
        .json(&json!({"username": username, "password": "password"}))
        .send()
        .await
        .context("signup request failed")?;
    let status = response.status();
    let payload = response
        .json::<ApiResponse<UserRecord>>()
        .await
        .context("signup response was not valid JSON")?;

    if status != StatusCode::CREATED || !payload.success {
        bail!("signup failed for {username}: {status} {}", payload.info);
    }

    let user = payload
        .body
        .ok_or_else(|| anyhow!("signup response missing body"))?;
    eprintln!(
        "benchmark user created username={} user_id={}",
        user.username, user.userid
    );
    Ok(user)
}

async fn deposit_usdc(
    client: &Client,
    settings: &Settings,
    user: &UserRecord,
    amount: i64,
) -> Result<()> {
    let idempotency_key = format!("{}-deposit", settings.run_id);
    command(
        client,
        settings,
        user,
        "POST",
        "/balance/",
        &idempotency_key,
        json!({"asset": "USDC", "amount": amount, "reference_id": idempotency_key}),
    )
    .await
    .context("deposit failed")?;
    Ok(())
}

async fn run_orders(
    client: &Client,
    settings: &Settings,
    user: &UserRecord,
    start_index: usize,
    count: usize,
) -> Result<Vec<u64>> {
    let semaphore = Arc::new(Semaphore::new(settings.concurrency));
    let mut tasks = JoinSet::new();

    for offset in 0..count {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let settings = settings.clone();
        let user = user.clone();
        let index = start_index + offset;

        tasks.spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            place_order(&client, &settings, &user, index).await?;
            Ok::<u64, anyhow::Error>(elapsed_nanos(started))
        });
    }

    let mut latencies = Vec::with_capacity(count);
    while let Some(result) = tasks.join_next().await {
        latencies.push(result??);
    }

    Ok(latencies)
}

async fn place_order(
    client: &Client,
    settings: &Settings,
    user: &UserRecord,
    index: usize,
) -> Result<()> {
    let idempotency_key = format!("{}-order-{index}", settings.run_id);
    command(
        client,
        settings,
        user,
        "POST",
        "/orders/",
        &idempotency_key,
        json!({
            "market_id": BENCH_MARKET.id,
            "market_name": BENCH_MARKET.name,
            "side": "LONG",
            "order_type": "LIMIT",
            "quantity": BENCH_MARKET.quantity,
            "price": BENCH_MARKET.price,
            "margin": BENCH_MARKET.margin,
            "margin_asset": "USDC",
            "leverage": 1
        }),
    )
    .await?;
    Ok(())
}

async fn command(
    client: &Client,
    settings: &Settings,
    user: &UserRecord,
    method: &str,
    path: &str,
    idempotency_key: &str,
    body: Value,
) -> Result<Value> {
    let url = format!("{}{}", settings.server_url, path);
    let request = match method {
        "POST" => client.post(url),
        _ => bail!("unsupported method {method}"),
    };
    let response = request
        .bearer_auth(&user.jwt_token)
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

    let final_body = if status == StatusCode::ACCEPTED {
        let request_id = body
            .get("request_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("queued command missing request_id for {path}"))?;
        poll_request(client, settings, user, request_id).await?
    } else {
        body
    };

    if final_body
        .get("complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Ok(final_body)
    } else {
        bail!("command {path} did not complete: {final_body}");
    }
}

async fn poll_request(
    client: &Client,
    settings: &Settings,
    user: &UserRecord,
    request_id: &str,
) -> Result<Value> {
    let deadline = Instant::now() + settings.request_timeout;
    let mut last_body = None;

    loop {
        let response = client
            .get(format!("{}/requests/{request_id}", settings.server_url))
            .bearer_auth(&user.jwt_token)
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
                last_body = Some(body);
            }
        }

        if Instant::now() >= deadline {
            bail!(
                "request {request_id} did not complete; last_body={}",
                last_body
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_else(|| String::from("<none>"))
            );
        }

        sleep(Duration::from_millis(10)).await;
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    let nanos = started.elapsed().as_nanos();
    nanos.min(u128::from(u64::MAX)) as u64
}

fn build_report(settings: &Settings, latencies: &[u64], duration: Duration) -> BenchReport {
    let duration_secs = duration.as_secs_f64();
    let latency_ns = latency_summary(latencies);
    let latency_ms = LatencySummary {
        p50: nanos_to_ms(latency_ns.p50),
        p90: nanos_to_ms(latency_ns.p90),
        p95: nanos_to_ms(latency_ns.p95),
        p99: nanos_to_ms(latency_ns.p99),
        p999: nanos_to_ms(latency_ns.p999),
        max: nanos_to_ms(latency_ns.max),
    };

    BenchReport {
        component: "exchange",
        scenario: "order_command_flow",
        transport: "json",
        commands: settings.commands,
        warmup: settings.warmup,
        concurrency: settings.concurrency,
        duration_ms: duration.as_secs_f64() * 1000.0,
        throughput_per_sec: settings.commands as f64 / duration_secs.max(f64::MIN_POSITIVE),
        latency_ns,
        latency_ms,
    }
}

fn latency_summary(values: &[u64]) -> LatencySummary<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    LatencySummary {
        p50: percentile(&sorted, 0.50),
        p90: percentile(&sorted, 0.90),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        p999: percentile(&sorted, 0.999),
        max: sorted.last().copied().unwrap_or_default(),
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() as f64 * percentile).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

fn nanos_to_ms(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}
