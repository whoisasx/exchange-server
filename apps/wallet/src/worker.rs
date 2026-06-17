use std::{error::Error, fmt};

use protocol::wallet::WalletCommand;

use crate::{
    processor::{WalletProcessResult, WalletProcessor},
    redpanda::WalletQueue,
    repository::WalletRepository,
    settings::WalletSettings,
};

#[derive(Debug)]
pub struct WalletError {
    message: String,
}

impl WalletError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for WalletError {}

#[derive(Clone)]
pub struct WalletWorker {
    settings: WalletSettings,
    processor: WalletProcessor,
    repository: WalletRepository,
}

impl WalletWorker {
    pub fn new(
        settings: WalletSettings,
        processor: WalletProcessor,
        repository: WalletRepository,
    ) -> Self {
        Self {
            settings,
            processor,
            repository,
        }
    }

    pub fn settings(&self) -> &WalletSettings {
        &self.settings
    }

    pub async fn process_command(
        &self,
        command: WalletCommand,
    ) -> Result<WalletProcessResult, WalletError> {
        self.processor
            .process_command(command)
            .await
            .map_err(|error| WalletError::new(format!("wallet command failed: {error:?}")))
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, WalletError> {
        self.repository
            .load_queue_offset(topic, partition)
            .await
            .map_err(|error| WalletError::new(format!("wallet offset load failed: {error:?}")))
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), WalletError> {
        self.repository
            .save_queue_offset(topic, partition, next_offset)
            .await
            .map_err(|error| WalletError::new(format!("wallet offset save failed: {error:?}")))
    }

    pub async fn run(&self) -> Result<(), WalletError> {
        println!(
            "wallet worker starting: consuming '{}' and forwarding valid orders to '{}'",
            self.settings.wallet_commands_topic, self.settings.engine_commands_topic
        );

        let queue = WalletQueue::new(&self.settings)
            .await
            .map_err(|error| WalletError::new(error.to_string()))?;

        queue
            .run(self.clone())
            .await
            .map_err(|error| WalletError::new(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use crate::settings::WalletSettings;
    use crate::{processor::WalletProcessor, repository::WalletRepository};

    use super::*;

    #[tokio::test]
    async fn worker_keeps_settings() {
        let settings = WalletSettings::from_env();
        let pool = PgPoolOptions::new()
            .connect_lazy(&settings.database_url)
            .expect("test database URL should be valid");
        let repository = WalletRepository::new(pool);
        let processor = WalletProcessor::new(repository.clone());
        let worker = WalletWorker::new(settings.clone(), processor, repository);

        assert_eq!(worker.settings(), &settings);
    }
}
