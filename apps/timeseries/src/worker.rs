use std::{error::Error, fmt};

use protocol::engine::EngineEvent;

use crate::{
    processor::TimeseriesProcessor,
    redpanda::TimeseriesQueue,
    repository::{TimeseriesRepository, TimeseriesRepositoryError},
    settings::TimeseriesSettings,
};

#[derive(Debug)]
pub struct TimeseriesError {
    message: String,
}

impl TimeseriesError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TimeseriesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for TimeseriesError {}

#[derive(Clone)]
pub struct TimeseriesWorker {
    settings: TimeseriesSettings,
    processor: TimeseriesProcessor,
    repository: TimeseriesRepository,
}

impl TimeseriesWorker {
    pub fn new(
        settings: TimeseriesSettings,
        processor: TimeseriesProcessor,
        repository: TimeseriesRepository,
    ) -> Self {
        Self {
            settings,
            processor,
            repository,
        }
    }

    pub fn settings(&self) -> &TimeseriesSettings {
        &self.settings
    }

    pub async fn process_engine_event(
        &self,
        event: EngineEvent,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
    ) -> Result<(), TimeseriesError> {
        match event {
            EngineEvent::TradeExecuted(trade) => {
                let candles = self.processor.candle_drafts(&trade);
                self.repository
                    .record_trade(topic, partition, offset, next_offset, &trade, &candles)
                    .await
                    .map_err(timeseries_repository_error)?;
            }
            EngineEvent::OrderOpened(_)
            | EngineEvent::OrderCancelled(_)
            | EngineEvent::OrderBookDelta(_) => {
                self.save_queue_offset(topic, partition, next_offset)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, TimeseriesError> {
        self.repository
            .load_queue_offset(topic, partition)
            .await
            .map_err(timeseries_repository_error)
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), TimeseriesError> {
        self.repository
            .save_queue_offset(topic, partition, next_offset)
            .await
            .map_err(timeseries_repository_error)
    }

    pub async fn run(&self) -> Result<(), TimeseriesError> {
        println!(
            "timeseries starting: group '{}' consuming '{}'",
            self.settings.consumer_group, self.settings.engine_events_topic
        );

        let queue = TimeseriesQueue::new(&self.settings)
            .await
            .map_err(|error| TimeseriesError::new(error.to_string()))?;

        queue
            .run(self.clone())
            .await
            .map_err(|error| TimeseriesError::new(error.to_string()))
    }
}

fn timeseries_repository_error(error: TimeseriesRepositoryError) -> TimeseriesError {
    TimeseriesError::new(format!("timeseries repository failed: {error:?}"))
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn worker_keeps_settings() {
        let settings = TimeseriesSettings::from_env();
        let pool = PgPoolOptions::new()
            .connect_lazy(&settings.database_url)
            .expect("test database URL should be valid");
        let repository = TimeseriesRepository::new(pool);
        let processor = TimeseriesProcessor::new();
        let worker = TimeseriesWorker::new(settings.clone(), processor, repository);

        assert_eq!(worker.settings(), &settings);
    }
}
