use std::{
    fmt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use protocol::engine::OrderBookSnapshotCreated;
use serde::{Deserialize, Serialize};
use tokio::fs;

const ARTIFACT_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone)]
pub struct LocalArchiveStore {
    root: PathBuf,
}

impl LocalArchiveStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub async fn write_snapshot_artifact(
        &self,
        bucket: &str,
        key_prefix: &str,
        snapshot: &OrderBookSnapshotCreated,
        source: SourceRecord,
    ) -> Result<ArchivedObject, ArchiveError> {
        let bucket = sanitize_segment(bucket);
        if bucket.is_empty() {
            return Err(ArchiveError::InvalidObjectKey(String::from(
                "archive bucket is empty",
            )));
        }

        let key = snapshot_object_key(key_prefix, snapshot);
        let path = object_path(&self.root, &bucket, &key);
        let artifact = SnapshotArchiveArtifact {
            artifact_schema_version: ARTIFACT_SCHEMA_VERSION,
            event_type: "OrderBookSnapshotCreated",
            source,
            snapshot,
        };
        let mut bytes = serde_json::to_vec_pretty(&artifact)?;
        bytes.push(b'\n');
        write_atomic(&path, &bytes).await?;

        Ok(ArchivedObject {
            bucket,
            key,
            path,
            byte_size: bytes.len() as u64,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LocalOffsetStore {
    root: PathBuf,
}

impl LocalOffsetStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub async fn load(&self, topic: &str, partition: i32) -> Result<Option<i64>, ArchiveError> {
        let path = self.offset_path(topic, partition);
        match fs::read_to_string(&path).await {
            Ok(contents) => {
                let offset = contents.trim().parse::<i64>().map_err(|error| {
                    ArchiveError::InvalidOffset(format!(
                        "invalid offset in {}: {error}",
                        path.display()
                    ))
                })?;
                Ok(Some(offset))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ArchiveError::Io(error)),
        }
    }

    pub async fn save(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ArchiveError> {
        let path = self.offset_path(topic, partition);
        write_atomic(&path, format!("{next_offset}\n").as_bytes()).await
    }

    fn offset_path(&self, topic: &str, partition: i32) -> PathBuf {
        self.root
            .join(sanitize_segment(topic))
            .join(format!("partition-{partition}.offset"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotArchiveArtifact<'a> {
    pub artifact_schema_version: i64,
    pub event_type: &'static str,
    pub source: SourceRecord,
    pub snapshot: &'a OrderBookSnapshotCreated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedObject {
    pub bucket: String,
    pub key: String,
    pub path: PathBuf,
    pub byte_size: u64,
}

#[derive(Debug)]
pub enum ArchiveError {
    InvalidObjectKey(String),
    InvalidOffset(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl From<std::io::Error> for ArchiveError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ArchiveError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObjectKey(message) => f.write_str(message),
            Self::InvalidOffset(message) => f.write_str(message),
            Self::Io(error) => write!(f, "local archive IO failed: {error}"),
            Self::Json(error) => write!(f, "archive artifact serialization failed: {error}"),
        }
    }
}

impl std::error::Error for ArchiveError {}

pub fn snapshot_object_key(prefix: &str, snapshot: &OrderBookSnapshotCreated) -> String {
    let mut segments = prefix_segments(prefix);
    segments.push(format!("market_id={}", snapshot.market_id));
    segments.push(format!("engine_sequence={}", snapshot.engine_sequence));
    segments.push(format!("{}.json", sanitize_segment(&snapshot.snapshot_id)));
    segments.join("/")
}

fn prefix_segments(prefix: &str) -> Vec<String> {
    prefix
        .split('/')
        .map(sanitize_segment)
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn object_path(root: &Path, bucket: &str, key: &str) -> PathBuf {
    let mut path = root.join(bucket);
    for segment in key.split('/') {
        path.push(segment);
    }
    path
}

fn sanitize_segment(segment: &str) -> String {
    segment
        .trim()
        .chars()
        .map(|character| match character {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '-' | '_' | '=' | '@' | '+' => character,
            _ => '_',
        })
        .collect::<String>()
        .trim_matches('.')
        .to_owned()
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ArchiveError> {
    let Some(parent) = path.parent() else {
        return Err(ArchiveError::InvalidObjectKey(format!(
            "object path has no parent: {}",
            path.display()
        )));
    };

    fs::create_dir_all(parent).await?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            ArchiveError::InvalidObjectKey(format!(
                "object path has no valid file name: {}",
                path.display()
            ))
        })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp_path = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));

    fs::write(&temp_path, bytes).await?;
    fs::rename(temp_path, path).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_object_key_uses_s3_shaped_segments() {
        let snapshot = snapshot_event("snapshot one/../bad");

        let key = snapshot_object_key("/orderbooks//snapshots/", &snapshot);

        assert_eq!(
            key,
            "orderbooks/snapshots/market_id=7/engine_sequence=42/snapshot_one_.._bad.json"
        );
    }

    #[tokio::test]
    async fn local_archive_writes_snapshot_artifact() {
        let root = test_dir("archive");
        let store = LocalArchiveStore::new(root.clone());
        let snapshot = snapshot_event("snap-42");
        let source = SourceRecord {
            topic: String::from("engine.events"),
            partition: 2,
            offset: 10,
            next_offset: 11,
        };

        let archived = store
            .write_snapshot_artifact("bucket", "prefix", &snapshot, source)
            .await
            .expect("artifact should write");

        let contents = fs::read_to_string(&archived.path)
            .await
            .expect("artifact should be readable");
        let value: serde_json::Value =
            serde_json::from_str(&contents).expect("artifact should be valid json");

        assert_eq!(archived.bucket, "bucket");
        assert_eq!(
            archived.key,
            "prefix/market_id=7/engine_sequence=42/snap-42.json"
        );
        assert_eq!(value["event_type"], "OrderBookSnapshotCreated");
        assert_eq!(value["source"]["next_offset"], 11);
        assert_eq!(value["snapshot"]["snapshot_id"], "snap-42");
        fs::remove_dir_all(root).await.ok();
    }

    #[tokio::test]
    async fn local_offsets_round_trip() {
        let root = test_dir("offsets");
        let store = LocalOffsetStore::new(root.clone());

        assert_eq!(
            store
                .load("engine.events", 0)
                .await
                .expect("missing offset should load"),
            None
        );

        store
            .save("engine.events", 0, 123)
            .await
            .expect("offset should save");

        assert_eq!(
            store
                .load("engine.events", 0)
                .await
                .expect("offset should load"),
            Some(123)
        );
        fs::remove_dir_all(root).await.ok();
    }

    fn snapshot_event(snapshot_id: &str) -> OrderBookSnapshotCreated {
        OrderBookSnapshotCreated {
            engine_event_id: Some(String::from("event-42")),
            market_id: 7,
            engine_sequence: 42,
            engine_timestamp_ms: 1_710_000_300_000,
            source_input_id: Some(String::from("snapshot-timer")),
            source_input_offset: Some(9),
            snapshot_id: String::from(snapshot_id),
            uri: String::from("s3://exchange-market-data/orderbooks/snap-42.json"),
            checksum_sha256: String::from("checksum"),
            byte_size: 4096,
            schema_version: 1,
            last_engine_sequence: 42,
        }
    }

    fn test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "exchange-orderbook-archiver-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
