use std::env;

use protocol::engine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeseriesSettings {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub consumer_group: String,
    pub engine_events_topic: String,
}

impl TimeseriesSettings {
    pub fn from_env() -> Self {
        Self {
            database_url: env_or_default(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/exchange",
            ),
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            consumer_group: env_or_default("TIMESERIES_CONSUMER_GROUP", "timeseries-service"),
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
            "__EXCHANGE_TIMESERIES_TEST_MISSING_TOPIC__",
            engine::ENGINE_EVENTS_TOPIC,
        );

        assert_eq!(value, engine::ENGINE_EVENTS_TOPIC);
    }
}
