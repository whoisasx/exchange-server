use std::{env, path::PathBuf};

use protocol::engine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderbookArchiverSettings {
    pub redpanda_brokers: String,
    pub consumer_group: String,
    pub engine_events_topic: String,
    pub archive_bucket: String,
    pub archive_key_prefix: String,
    pub archive_local_root: PathBuf,
    pub archive_endpoint: Option<String>,
    pub archive_region: Option<String>,
    pub offset_local_root: PathBuf,
}

impl OrderbookArchiverSettings {
    pub fn from_env() -> Self {
        Self {
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            consumer_group: env_or_default(
                "ORDERBOOK_ARCHIVER_CONSUMER_GROUP",
                "orderbook-archiver",
            ),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
            archive_bucket: env_or_default("ORDERBOOK_ARCHIVE_BUCKET", "exchange-market-data"),
            archive_key_prefix: env_or_default(
                "ORDERBOOK_ARCHIVE_KEY_PREFIX",
                "orderbooks/snapshots",
            ),
            archive_local_root: PathBuf::from(env_or_default(
                "ORDERBOOK_ARCHIVE_LOCAL_ROOT",
                ".data/orderbook-archiver/objects",
            )),
            archive_endpoint: env_optional("ORDERBOOK_ARCHIVE_ENDPOINT"),
            archive_region: env_optional("ORDERBOOK_ARCHIVE_REGION"),
            offset_local_root: PathBuf::from(env_or_default(
                "ORDERBOOK_ARCHIVER_OFFSET_LOCAL_ROOT",
                ".data/orderbook-archiver/offsets",
            )),
        }
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| String::from(default))
}

fn env_optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_or_default_uses_default_for_missing_key() {
        let value = env_or_default(
            "__EXCHANGE_ORDERBOOK_ARCHIVER_TEST_MISSING_TOPIC__",
            engine::ENGINE_EVENTS_TOPIC,
        );

        assert_eq!(value, engine::ENGINE_EVENTS_TOPIC);
    }

    #[test]
    fn env_optional_ignores_missing_key() {
        assert_eq!(
            env_optional("__EXCHANGE_ORDERBOOK_ARCHIVER_TEST_MISSING_REGION__"),
            None
        );
    }
}
