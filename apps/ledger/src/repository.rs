use db::dto::AssetType;
use protocol::common::Asset;
use sqlx::{Pool, Postgres, Row};

use crate::processor::LedgerRecord;

const SAVE_QUEUE_OFFSET_SQL: &str = r#"
INSERT INTO ledger_offsets(topic, partition, next_offset)
VALUES($1,$2,$3)
ON CONFLICT(topic, partition)
DO UPDATE
SET next_offset=EXCLUDED.next_offset,
    updated_at=NOW()
WHERE ledger_offsets.next_offset < EXCLUDED.next_offset
"#;

const INSERT_LEDGER_EVENT_SQL: &str = r#"
INSERT INTO ledger_events(
    topic,
    partition,
    offset_value,
    logical_event_id,
    event_type,
    user_id,
    payload
)
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT DO NOTHING
RETURNING event_id
"#;

#[derive(Debug)]
pub enum LedgerRepositoryError {
    Storage(sqlx::Error),
}

impl From<sqlx::Error> for LedgerRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone)]
pub struct LedgerRepository {
    pool: Pool<Postgres>,
}

impl LedgerRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, LedgerRepositoryError> {
        let offset = sqlx::query(
            r#"
            SELECT next_offset
            FROM ledger_offsets
            WHERE topic=$1 AND partition=$2
            "#,
        )
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.get("next_offset"));

        Ok(offset)
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), LedgerRepositoryError> {
        let mut tx = self.pool.begin().await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn record_wallet_event(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
        record: &LedgerRecord,
    ) -> Result<bool, LedgerRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let event_id = sqlx::query_scalar::<_, i64>(INSERT_LEDGER_EVENT_SQL)
            .bind(topic)
            .bind(partition)
            .bind(offset)
            .bind(record.logical_event_id.as_deref())
            .bind(record.event_type)
            .bind(record.user_id)
            .bind(&record.payload)
            .fetch_optional(&mut *tx)
            .await?;

        let inserted = if let Some(event_id) = event_id {
            for entry in &record.entries {
                sqlx::query(
                    r#"
                    INSERT INTO ledger_entries(
                        event_id,
                        user_id,
                        asset,
                        kind,
                        total_delta,
                        locked_delta,
                        reference_id
                    )
                    VALUES($1,$2,$3,$4,$5,$6,$7)
                    "#,
                )
                .bind(event_id)
                .bind(entry.user_id)
                .bind(asset_to_db(entry.asset))
                .bind(&entry.kind)
                .bind(entry.total_delta)
                .bind(entry.locked_delta)
                .bind(&entry.reference_id)
                .execute(&mut *tx)
                .await?;
            }
            true
        } else {
            false
        };

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(inserted)
    }
}

async fn save_queue_offset_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> Result<(), LedgerRepositoryError> {
    sqlx::query(SAVE_QUEUE_OFFSET_SQL)
        .bind(topic)
        .bind(partition)
        .bind(next_offset)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

pub fn asset_to_db(asset: Asset) -> AssetType {
    match asset {
        Asset::USDC => AssetType::USDC,
        Asset::USDT => AssetType::USDT,
        Asset::SOL => AssetType::SOL,
        Asset::ETH => AssetType::ETH,
        Asset::BTC => AssetType::BTC,
        Asset::PERP => AssetType::PERP,
        Asset::HYP => AssetType::HYP,
    }
}

#[cfg(test)]
mod tests {
    use db::dto::AssetType;
    use protocol::common::Asset;

    use super::{INSERT_LEDGER_EVENT_SQL, SAVE_QUEUE_OFFSET_SQL, asset_to_db};

    #[test]
    fn asset_mapping_covers_known_assets() {
        assert_eq!(asset_to_db(Asset::USDC), AssetType::USDC);
        assert_eq!(asset_to_db(Asset::SOL), AssetType::SOL);
        assert_eq!(asset_to_db(Asset::HYP), AssetType::HYP);
    }

    #[test]
    fn queue_offset_upsert_only_advances_offset() {
        let sql = SAVE_QUEUE_OFFSET_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("ON CONFLICT(topic, partition) DO UPDATE"));
        assert!(sql.contains("WHERE ledger_offsets.next_offset < EXCLUDED.next_offset"));
    }

    #[test]
    fn ledger_event_insert_is_idempotent_by_topic_offset_or_logical_event_id() {
        let sql = INSERT_LEDGER_EVENT_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("logical_event_id"));
        assert!(sql.contains("ON CONFLICT DO NOTHING"));
        assert!(sql.contains("RETURNING event_id"));
    }
}
