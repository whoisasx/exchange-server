use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub timeseries_database_url: Option<String>,
    pub server_url: String,
    pub server_port: u16,
    pub server_host: String,
    pub jwt_secret: String,
    pub redpanda_brokers: String,
    pub wallet_commands_topic: String,
    pub wallet_replies_topic: String,
    pub engine_replies_topic: String,
    pub server_reply_partition: i32,
    pub request_wait_timeout_ms: u64,
}

impl Config {
    pub fn init() -> Config {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let timeseries_database_url = Some(env_required("TIMESERIES_DATABASE_URL"));
        let server_url = env::var("SERVER_URL").expect("SERVER_URL must be set");
        let server_port = env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .expect("SERVER_PORT must be set");
        let server_host = env::var("SERVER_HOST").expect("SERVER_HOST must be set");
        let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
        let redpanda_brokers =
            env::var("REDPANDA_BROKERS").unwrap_or_else(|_| String::from("localhost:9092"));
        let wallet_commands_topic =
            env::var("WALLET_COMMANDS_TOPIC").unwrap_or_else(|_| String::from("wallet.commands"));
        let wallet_replies_topic =
            env::var("WALLET_REPLIES_TOPIC").unwrap_or_else(|_| String::from("wallet.replies"));
        let engine_replies_topic =
            env::var("ENGINE_REPLIES_TOPIC").unwrap_or_else(|_| String::from("engine.replies"));
        let server_reply_partition = env::var("SERVER_REPLY_PARTITION")
            .unwrap_or_else(|_| String::from("0"))
            .parse::<i32>()
            .expect("SERVER_REPLY_PARTITION must be a valid i32");
        let request_wait_timeout_ms = env::var("REQUEST_WAIT_TIMEOUT_MS")
            .unwrap_or_else(|_| String::from("5000"))
            .parse::<u64>()
            .expect("REQUEST_WAIT_TIMEOUT_MS must be a valid u64");

        Config {
            database_url,
            timeseries_database_url,
            server_url,
            server_port,
            server_host,
            jwt_secret,
            redpanda_brokers,
            wallet_commands_topic,
            wallet_replies_topic,
            engine_replies_topic,
            server_reply_partition,
            request_wait_timeout_ms,
        }
    }
}

fn env_required(key: &str) -> String {
    required_value(key, env::var(key).ok())
}

fn required_value(key: &str, value: Option<String>) -> String {
    value
        .and_then(non_empty)
        .unwrap_or_else(|| panic!("{key} must be set"))
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_value_accepts_timeseries_database_url() {
        let database_url = required_value(
            "TIMESERIES_DATABASE_URL",
            Some(String::from("postgres://localhost/timeseries")),
        );

        assert_eq!(database_url, "postgres://localhost/timeseries");
    }

    #[test]
    #[should_panic(expected = "TIMESERIES_DATABASE_URL must be set")]
    fn required_value_rejects_missing_timeseries_database_url() {
        required_value("TIMESERIES_DATABASE_URL", None);
    }

    #[test]
    #[should_panic(expected = "TIMESERIES_DATABASE_URL must be set")]
    fn required_value_rejects_blank_timeseries_database_url() {
        required_value("TIMESERIES_DATABASE_URL", Some(String::from("  ")));
    }
}
