use std::env;

use protocol::engine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeseriesSettings {
    pub database_url: String,
    pub redpanda_brokers: String,
    pub engine_events_topic: String,
}

impl TimeseriesSettings {
    pub fn from_env() -> Self {
        Self {
            database_url: required_env("TIMESERIES_DATABASE_URL"),
            redpanda_brokers: env_or_default("REDPANDA_BROKERS", "localhost:9092"),
            engine_events_topic: env_or_default("ENGINE_EVENTS_TOPIC", engine::ENGINE_EVENTS_TOPIC),
        }
    }
}

fn required_env(key: &str) -> String {
    required_value(key, env::var(key).ok())
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn required_value(key: &str, value: Option<String>) -> String {
    value
        .and_then(non_empty)
        .unwrap_or_else(|| panic!("{key} must be set"))
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
