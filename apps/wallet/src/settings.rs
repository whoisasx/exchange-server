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
            engine_commands_topic: env_or_default(
                "ENGINE_COMMANDS_TOPIC",
                engine::ENGINE_COMMANDS_TOPIC,
            ),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
        }
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| String::from(default))
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
}
