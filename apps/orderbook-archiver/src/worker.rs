use std::{error::Error, fmt};

use protocol::engine::EngineEvent;

use crate::{
    archive::{ArchiveError, LocalArchiveStore, LocalOffsetStore, SourceRecord},
    redpanda::OrderbookArchiverQueue,
    settings::OrderbookArchiverSettings,
};

#[derive(Debug)]
pub struct OrderbookArchiverError {
    message: String,
}

impl OrderbookArchiverError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for OrderbookArchiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for OrderbookArchiverError {}

#[derive(Clone)]
pub struct OrderbookArchiverWorker {
    settings: OrderbookArchiverSettings,
    archive_store: LocalArchiveStore,
    offset_store: LocalOffsetStore,
}

impl OrderbookArchiverWorker {
    pub fn new(
        settings: OrderbookArchiverSettings,
        archive_store: LocalArchiveStore,
        offset_store: LocalOffsetStore,
    ) -> Self {
        Self {
            settings,
            archive_store,
            offset_store,
        }
    }

    pub async fn process_engine_event(
        &self,
        event: EngineEvent,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
    ) -> Result<(), OrderbookArchiverError> {
        if let EngineEvent::OrderBookSnapshotCreated(snapshot) = event {
            let source = SourceRecord {
                topic: String::from(topic),
                partition,
                offset,
                next_offset,
            };
            let archived = self
                .archive_store
                .write_snapshot_artifact(
                    &self.settings.archive_bucket,
                    &self.settings.archive_key_prefix,
                    &snapshot,
                    source,
                )
                .await
                .map_err(archive_error)?;

            println!(
                "archived orderbook snapshot '{}' to {}/{} ({} bytes)",
                snapshot.snapshot_id, archived.bucket, archived.key, archived.byte_size
            );
        }

        self.save_queue_offset(topic, partition, next_offset).await
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, OrderbookArchiverError> {
        self.offset_store
            .load(topic, partition)
            .await
            .map_err(archive_error)
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), OrderbookArchiverError> {
        self.offset_store
            .save(topic, partition, next_offset)
            .await
            .map_err(archive_error)
    }

    pub async fn run(&self) -> Result<(), OrderbookArchiverError> {
        println!(
            "orderbook archiver starting: group '{}' consuming '{}' into bucket '{}' prefix '{}'",
            self.settings.consumer_group,
            self.settings.engine_events_topic,
            self.settings.archive_bucket,
            self.settings.archive_key_prefix
        );

        let queue = OrderbookArchiverQueue::new(&self.settings)
            .await
            .map_err(|error| OrderbookArchiverError::new(error.to_string()))?;

        queue
            .run(self.clone())
            .await
            .map_err(|error| OrderbookArchiverError::new(error.to_string()))
    }
}

fn archive_error(error: ArchiveError) -> OrderbookArchiverError {
    OrderbookArchiverError::new(format!("orderbook archive failed: {error}"))
}
