use std::env;

use protocol::wallet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerSettings {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub wallet_events_topic: String,
}

impl LedgerSettings {
    pub fn from_env() -> Self {
        Self {
            database_url: env_or_default(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/exchange",
            ),
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
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
            "__EXCHANGE_LEDGER_TEST_MISSING_TOPIC__",
            wallet::WALLET_EVENTS_TOPIC,
        );

        assert_eq!(value, wallet::WALLET_EVENTS_TOPIC);
    }
}
