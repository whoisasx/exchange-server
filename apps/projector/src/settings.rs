use std::env;

use protocol::{engine, wallet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectorSettings {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub engine_input_topic: String,
    pub engine_replies_topic: String,
    pub engine_events_topic: String,
    pub wallet_events_topic: String,
}

impl ProjectorSettings {
    pub fn from_env() -> Self {
        Self {
            database_url: env_or_default(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/exchange",
            ),
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            engine_input_topic: env_or_default("ENGINE_INPUT_TOPIC", engine::ENGINE_INPUT_TOPIC),
            engine_replies_topic: env_or_default(
                "ENGINE_REPLIES_TOPIC",
                engine::ENGINE_REPLIES_TOPIC,
            ),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
            wallet_events_topic: env_or_default("WALLET_EVENTS_TOPIC", wallet::WALLET_EVENTS_TOPIC),
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
            "__EXCHANGE_PROJECTOR_TEST_MISSING_TOPIC__",
            engine::ENGINE_EVENTS_TOPIC,
        );

        assert_eq!(value, engine::ENGINE_EVENTS_TOPIC);
    }
}
