use std::env;

use protocol::{engine, wallet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletSettings {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub consumer_group: String,
    pub wallet_commands_topic: String,
    pub wallet_replies_topic: String,
    pub wallet_events_topic: String,
    pub engine_commands_topic: String,
    pub engine_events_topic: String,
    pub wallet_outbox_batch_limit: i64,
    pub wallet_outbox_stale_after_seconds: i64,
    pub wallet_outbox_metrics_interval_seconds: i64,
    pub wallet_outbox_alert_pending_count: i64,
    pub wallet_outbox_alert_oldest_pending_seconds: i64,
}

impl WalletSettings {
    pub fn from_env() -> Self {
        Self {
            database_url: env_or_default(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/exchange",
            ),
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            consumer_group: env_or_default("WALLET_CONSUMER_GROUP", "wallet-service"),
            wallet_commands_topic: env_or_default(
                "WALLET_COMMANDS_TOPIC",
                wallet::WALLET_COMMANDS_TOPIC,
            ),
            wallet_replies_topic: env_or_default(
                "WALLET_REPLIES_TOPIC",
                wallet::WALLET_REPLIES_TOPIC,
            ),
            wallet_events_topic: env_or_default("WALLET_EVENTS_TOPIC", wallet::WALLET_EVENTS_TOPIC),
            engine_commands_topic: env_or_default_with_legacy(
                "ENGINE_INPUT_TOPIC",
                "ENGINE_COMMANDS_TOPIC",
                engine::ENGINE_INPUT_TOPIC,
            ),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
            wallet_outbox_batch_limit: env_i64_or_default("WALLET_OUTBOX_BATCH_LIMIT", 100),
            wallet_outbox_stale_after_seconds: env_i64_or_default(
                "WALLET_OUTBOX_STALE_AFTER_SECONDS",
                60,
            ),
            wallet_outbox_metrics_interval_seconds: env_i64_or_default(
                "WALLET_OUTBOX_METRICS_INTERVAL_SECONDS",
                30,
            ),
            wallet_outbox_alert_pending_count: env_i64_or_default(
                "WALLET_OUTBOX_ALERT_PENDING_COUNT",
                1_000,
            ),
            wallet_outbox_alert_oldest_pending_seconds: env_i64_or_default(
                "WALLET_OUTBOX_ALERT_OLDEST_PENDING_SECONDS",
                60,
            ),
        }
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| String::from(default))
}

fn env_or_default_with_legacy(key: &str, legacy_key: &str, default: &str) -> String {
    env::var(key)
        .or_else(|_| env::var(legacy_key))
        .unwrap_or_else(|_| String::from(default))
}

fn env_i64_or_default(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_or_default_uses_default_for_missing_key() {
        let value = env_or_default(
            "__EXCHANGE_WALLET_TEST_MISSING_TOPIC__",
            wallet::WALLET_COMMANDS_TOPIC,
        );

        assert_eq!(value, wallet::WALLET_COMMANDS_TOPIC);
    }

    #[test]
    fn env_or_default_with_legacy_prefers_target_default() {
        let value = env_or_default_with_legacy(
            "__EXCHANGE_WALLET_TEST_MISSING_ENGINE_INPUT_TOPIC__",
            "__EXCHANGE_WALLET_TEST_MISSING_ENGINE_COMMANDS_TOPIC__",
            engine::ENGINE_INPUT_TOPIC,
        );

        assert_eq!(value, engine::ENGINE_INPUT_TOPIC);
    }
}
