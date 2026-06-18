use chrono::{DateTime, Utc};

use crate::{dto::CandleRow, pool::pool};

pub const CANDLE_INTERVALS: [&str; 5] = ["1m", "5m", "15m", "1h", "1d"];
pub const DEFAULT_CANDLE_LIMIT: i64 = 500;
pub const MAX_CANDLE_LIMIT: i64 = 1000;

pub fn is_supported_interval(interval: &str) -> bool {
    CANDLE_INTERVALS.contains(&interval)
}

pub async fn get_market_candles(
    market_id: i64,
    interval: &str,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<CandleRow>, sqlx::Error> {
    let candles = sqlx::query_as::<_, CandleRow>(
        r#"
        SELECT *
        FROM (
            SELECT
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
                last_engine_sequence,
                created_at,
                updated_at
            FROM candles
            WHERE market_id=$1
              AND interval=$2
              AND ($3::TIMESTAMPTZ IS NULL OR bucket_start >= $3)
              AND ($4::TIMESTAMPTZ IS NULL OR bucket_start < $4)
            ORDER BY bucket_start DESC
            LIMIT $5
        ) recent
        ORDER BY bucket_start ASC
        "#,
    )
    .bind(market_id)
    .bind(interval)
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(pool())
    .await?;

    Ok(candles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_intervals_are_explicit() {
        assert!(is_supported_interval("1m"));
        assert!(is_supported_interval("1d"));
        assert!(!is_supported_interval("2m"));
    }
}
