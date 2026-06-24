use protocol::engine::TradeExecuted;
use sqlx::{Pool, Postgres, Row};

use crate::processor::CandleDraft;

const SAVE_QUEUE_OFFSET_SQL: &str = r#"
INSERT INTO timeseries_offsets(topic, partition, next_offset)
VALUES($1,$2,$3)
ON CONFLICT(topic, partition)
DO UPDATE
SET next_offset=EXCLUDED.next_offset,
    updated_at=NOW()
WHERE timeseries_offsets.next_offset < EXCLUDED.next_offset
"#;

#[derive(Debug)]
pub enum TimeseriesRepositoryError {
    Storage(sqlx::Error),
}

impl From<sqlx::Error> for TimeseriesRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone)]
pub struct TimeseriesRepository {
    pool: Pool<Postgres>,
}

impl TimeseriesRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, TimeseriesRepositoryError> {
        let offset = sqlx::query(
            r#"
            SELECT next_offset
            FROM timeseries_offsets
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
    ) -> Result<(), TimeseriesRepositoryError> {
        let mut tx = self.pool.begin().await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn record_trade(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
        trade: &TradeExecuted,
        candles: &[CandleDraft],
    ) -> Result<bool, TimeseriesRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO timeseries_trades(
                market_id,
                engine_sequence,
                fill_id,
                engine_timestamp_ms,
                executed_at,
                price,
                quantity,
                topic,
                partition,
                offset_value
            )
            VALUES($1,$2,$3,$4,TO_TIMESTAMP($4::DOUBLE PRECISION / 1000.0),$5,$6,$7,$8,$9)
            ON CONFLICT(market_id, engine_sequence) DO NOTHING
            RETURNING engine_sequence
            "#,
        )
        .bind(trade.market_id)
        .bind(trade.engine_sequence)
        .bind(trade.fill_id)
        .bind(trade.engine_timestamp_ms)
        .bind(trade.price)
        .bind(trade.quantity)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();

        if inserted {
            for candle in candles {
                upsert_candle_in_tx(&mut tx, candle).await?;
            }
        }

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
) -> Result<(), TimeseriesRepositoryError> {
    sqlx::query(SAVE_QUEUE_OFFSET_SQL)
        .bind(topic)
        .bind(partition)
        .bind(next_offset)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SAVE_QUEUE_OFFSET_SQL;

    #[test]
    fn queue_offset_upsert_only_advances_offset() {
        let sql = SAVE_QUEUE_OFFSET_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("ON CONFLICT(topic, partition) DO UPDATE"));
        assert!(sql.contains("WHERE timeseries_offsets.next_offset < EXCLUDED.next_offset"));
    }
}

async fn upsert_candle_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    candle: &CandleDraft,
) -> Result<(), TimeseriesRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO candles(
            market_id,
            interval,
            bucket_start,
            open,
            high,
            low,
            close,
            volume,
            trade_count,
            first_engine_sequence,
            last_engine_sequence
        )
        VALUES($1,$2,TO_TIMESTAMP($3::DOUBLE PRECISION / 1000.0),$4,$4,$4,$4,$5,1,$6,$6)
        ON CONFLICT(market_id, interval, bucket_start)
        DO UPDATE
        SET open=CASE
                WHEN EXCLUDED.first_engine_sequence < candles.first_engine_sequence
                    THEN EXCLUDED.open
                ELSE candles.open
            END,
            high=GREATEST(candles.high, EXCLUDED.high),
            low=LEAST(candles.low, EXCLUDED.low),
            close=CASE
                WHEN EXCLUDED.last_engine_sequence > candles.last_engine_sequence
                    THEN EXCLUDED.close
                ELSE candles.close
            END,
            volume=candles.volume + EXCLUDED.volume,
            trade_count=candles.trade_count + EXCLUDED.trade_count,
            first_engine_sequence=LEAST(
                candles.first_engine_sequence,
                EXCLUDED.first_engine_sequence
            ),
            last_engine_sequence=GREATEST(
                candles.last_engine_sequence,
                EXCLUDED.last_engine_sequence
            ),
            updated_at=NOW()
        "#,
    )
    .bind(candle.market_id)
    .bind(candle.interval)
    .bind(candle.bucket_start_ms)
    .bind(candle.price)
    .bind(candle.quantity)
    .bind(candle.engine_sequence)
    .execute(&mut **tx)
    .await?;

    Ok(())
}
