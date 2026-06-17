use std::{error::Error, fmt};

use protocol::wallet::WalletEvent;

use crate::{
    processor::LedgerProcessor,
    redpanda::LedgerQueue,
    repository::{LedgerRepository, LedgerRepositoryError},
    settings::LedgerSettings,
};

#[derive(Debug)]
pub struct LedgerError {
    message: String,
}

impl LedgerError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for LedgerError {}

#[derive(Clone)]
pub struct LedgerWorker {
    settings: LedgerSettings,
    processor: LedgerProcessor,
    repository: LedgerRepository,
}

impl LedgerWorker {
    pub fn new(
        settings: LedgerSettings,
        processor: LedgerProcessor,
        repository: LedgerRepository,
    ) -> Self {
        Self {
            settings,
            processor,
            repository,
        }
    }

    pub fn settings(&self) -> &LedgerSettings {
        &self.settings
    }

    pub async fn process_wallet_event(
        &self,
        event: WalletEvent,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
    ) -> Result<(), LedgerError> {
        let record = self
            .processor
            .process_wallet_event(&event)
            .map_err(|error| LedgerError::new(error.to_string()))?;
        self.repository
            .record_wallet_event(topic, partition, offset, next_offset, &record)
            .await
            .map_err(ledger_repository_error)?;

        Ok(())
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, LedgerError> {
        self.repository
            .load_queue_offset(topic, partition)
            .await
            .map_err(ledger_repository_error)
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), LedgerError> {
        self.repository
            .save_queue_offset(topic, partition, next_offset)
            .await
            .map_err(ledger_repository_error)
    }

    pub async fn run(&self) -> Result<(), LedgerError> {
        println!(
            "ledger starting: group '{}' consuming '{}'",
            self.settings.consumer_group, self.settings.wallet_events_topic
        );

        let queue = LedgerQueue::new(&self.settings)
            .await
            .map_err(|error| LedgerError::new(error.to_string()))?;

        queue
            .run(self.clone())
            .await
            .map_err(|error| LedgerError::new(error.to_string()))
    }
}

fn ledger_repository_error(error: LedgerRepositoryError) -> LedgerError {
    LedgerError::new(format!("ledger repository failed: {error:?}"))
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn worker_keeps_settings() {
        let settings = LedgerSettings::from_env();
        let pool = PgPoolOptions::new()
            .connect_lazy(&settings.database_url)
            .expect("test database URL should be valid");
        let repository = LedgerRepository::new(pool);
        let processor = LedgerProcessor::new();
        let worker = LedgerWorker::new(settings.clone(), processor, repository);

        assert_eq!(worker.settings(), &settings);
    }
}
