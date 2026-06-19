use std::env;

use protocol::{engine, wallet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeEngineSettings {
    pub redpanda_brokers: String,
    pub engine_input_topic: String,
    pub engine_replies_topic: String,
    pub engine_events_topic: String,
    pub wallet_events_topic: String,
    pub order_id_start: i64,
    pub fill_id_start: i64,
}

impl FakeEngineSettings {
    pub fn from_env() -> Self {
        Self {
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            engine_input_topic: engine_input_topic_from_env(),
            engine_replies_topic: env_or_default(
                "ENGINE_REPLIES_TOPIC",
                engine::ENGINE_REPLIES_TOPIC,
            ),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
            wallet_events_topic: env_or_default("WALLET_EVENTS_TOPIC", wallet::WALLET_EVENTS_TOPIC),
            order_id_start: env_i64_or_default("FAKE_ENGINE_ORDER_ID_START", 1_000_000),
            fill_id_start: env_i64_or_default("FAKE_ENGINE_FILL_ID_START", 2_000_000),
        }
    }
}

fn engine_input_topic_from_env() -> String {
    env::var("ENGINE_INPUT_TOPIC")
        .or_else(|_| env::var("ENGINE_COMMANDS_TOPIC"))
        .unwrap_or_else(|_| String::from(engine::ENGINE_INPUT_TOPIC))
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| String::from(default))
}

fn env_i64_or_default(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_i64_or_default_uses_default_for_missing_key() {
        let value = env_i64_or_default("__EXCHANGE_FAKE_ENGINE_MISSING_ID__", 42);

        assert_eq!(value, 42);
    }
}
