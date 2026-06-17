use std::env;

use protocol::{engine, wallet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsSettings {
    pub redpanda_brokers: String,
    pub ws_host: String,
    pub ws_port: u16,
    pub jwt_secret: String,
    pub engine_events_topic: String,
    pub wallet_events_topic: String,
}

impl WsSettings {
    pub fn from_env() -> Self {
        Self {
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            ws_host: env_or_default("WS_HOST", "127.0.0.1"),
            ws_port: env_u16_or_default("WS_PORT", 8081),
            jwt_secret: env_or_default("JWT_SECRET", "change-me"),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
            wallet_events_topic: env_or_default("WALLET_EVENTS_TOPIC", wallet::WALLET_EVENTS_TOPIC),
        }
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| String::from(default))
}

fn env_u16_or_default(key: &str, default: u16) -> u16 {
    env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u16>()
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_u16_or_default_uses_default_for_missing_key() {
        assert_eq!(
            env_u16_or_default("__EXCHANGE_WS_TEST_MISSING_PORT__", 8081),
            8081
        );
    }

    #[test]
    fn env_or_default_uses_default_for_missing_key() {
        assert_eq!(
            env_or_default(
                "__EXCHANGE_WS_TEST_MISSING_TOPIC__",
                engine::ENGINE_EVENTS_TOPIC
            ),
            engine::ENGINE_EVENTS_TOPIC
        );
    }
}
