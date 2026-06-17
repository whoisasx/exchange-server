use std::{error::Error, fmt};

use protocol::{
    engine::{EngineCommand, EngineEvent, EngineReply},
    wallet::WalletEvent,
};

use crate::{
    processor::ProjectorProcessor,
    redpanda::ProjectorQueue,
    repository::{ProjectorRepository, ProjectorRepositoryError},
    settings::ProjectorSettings,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectorErrorKind {
    MissingOrderContext,
    Other,
}

#[derive(Debug)]
pub struct ProjectorError {
    kind: ProjectorErrorKind,
    message: String,
}

impl ProjectorError {
    pub fn new(kind: ProjectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn is_missing_order_context(&self) -> bool {
        self.kind == ProjectorErrorKind::MissingOrderContext
    }
}

impl fmt::Display for ProjectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ProjectorError {}

#[derive(Clone)]
pub struct ProjectorWorker {
    settings: ProjectorSettings,
    processor: ProjectorProcessor,
    repository: ProjectorRepository,
}

impl ProjectorWorker {
    pub fn new(
        settings: ProjectorSettings,
        processor: ProjectorProcessor,
        repository: ProjectorRepository,
    ) -> Self {
        Self {
            settings,
            processor,
            repository,
        }
    }

    pub fn settings(&self) -> &ProjectorSettings {
        &self.settings
    }

    pub async fn process_engine_command(
        &self,
        command: EngineCommand,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorError> {
        self.processor
            .process_engine_command(command, topic, partition, next_offset)
            .await
            .map_err(projector_error)
    }

    pub async fn process_engine_reply(
        &self,
        reply: EngineReply,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorError> {
        self.processor
            .process_engine_reply(reply, topic, partition, next_offset)
            .await
            .map_err(projector_error)
    }

    pub async fn process_engine_event(
        &self,
        event: EngineEvent,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorError> {
        self.processor
            .process_engine_event(event, topic, partition, next_offset)
            .await
            .map_err(projector_error)
    }

    pub async fn process_wallet_event(
        &self,
        event: WalletEvent,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorError> {
        self.processor
            .process_wallet_event(event, topic, partition, next_offset)
            .await
            .map_err(projector_error)
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ProjectorError> {
        self.repository
            .load_queue_offset(topic, partition)
            .await
            .map_err(projector_error)
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorError> {
        self.repository
            .save_queue_offset(topic, partition, next_offset)
            .await
            .map_err(projector_error)
    }

    pub async fn run(&self) -> Result<(), ProjectorError> {
        println!(
            "projector starting: group '{}' consuming '{}', '{}', '{}', and '{}'",
            self.settings.consumer_group,
            self.settings.engine_commands_topic,
            self.settings.engine_replies_topic,
            self.settings.engine_events_topic,
            self.settings.wallet_events_topic
        );

        let queue = ProjectorQueue::new(&self.settings)
            .await
            .map_err(|error| ProjectorError::new(ProjectorErrorKind::Other, error.to_string()))?;

        queue
            .run(self.clone())
            .await
            .map_err(|error| ProjectorError::new(ProjectorErrorKind::Other, error.to_string()))
    }
}

fn projector_error(error: ProjectorRepositoryError) -> ProjectorError {
    let kind = if error.is_missing_order_context() {
        ProjectorErrorKind::MissingOrderContext
    } else {
        ProjectorErrorKind::Other
    };

    ProjectorError::new(kind, format!("projector repository failed: {error:?}"))
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn worker_keeps_settings() {
        let settings = ProjectorSettings::from_env();
        let pool = PgPoolOptions::new()
            .connect_lazy(&settings.database_url)
            .expect("test database URL should be valid");
        let repository = ProjectorRepository::new(pool);
        let processor = ProjectorProcessor::new(repository.clone());
        let worker = ProjectorWorker::new(settings.clone(), processor, repository);

        assert_eq!(worker.settings(), &settings);
    }
}
