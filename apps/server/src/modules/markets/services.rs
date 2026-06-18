use chrono::{DateTime, Utc};
use db::{
    candles::{DEFAULT_CANDLE_LIMIT, MAX_CANDLE_LIMIT, get_market_candles, is_supported_interval},
    dto::CandleRow,
};

#[derive(Debug, PartialEq, Eq)]
pub enum MarketServiceError {
    InvalidInterval,
    InvalidLimit,
    InvalidTimestamp,
    InvalidTimeRange,
    Storage,
}

pub async fn get_candles(
    market_id: i64,
    interval: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<CandleRow>, MarketServiceError> {
    if !is_supported_interval(interval) {
        return Err(MarketServiceError::InvalidInterval);
    }

    let limit = limit.unwrap_or(DEFAULT_CANDLE_LIMIT);
    if !(1..=MAX_CANDLE_LIMIT).contains(&limit) {
        return Err(MarketServiceError::InvalidLimit);
    }

    let start = timestamp_millis(start_ms)?;
    let end = timestamp_millis(end_ms)?;
    if let (Some(start), Some(end)) = (start, end) {
        if start >= end {
            return Err(MarketServiceError::InvalidTimeRange);
        }
    }

    get_market_candles(market_id, interval, start, end, limit)
        .await
        .map_err(|_| MarketServiceError::Storage)
}

fn timestamp_millis(value: Option<i64>) -> Result<Option<DateTime<Utc>>, MarketServiceError> {
    value
        .map(|timestamp| {
            DateTime::<Utc>::from_timestamp_millis(timestamp)
                .ok_or(MarketServiceError::InvalidTimestamp)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_candles_rejects_invalid_interval() {
        let result = get_candles(1, "2m", None, None, None).await;

        assert_eq!(result.unwrap_err(), MarketServiceError::InvalidInterval);
    }

    #[tokio::test]
    async fn get_candles_rejects_invalid_limit() {
        let result = get_candles(1, "1m", None, None, Some(MAX_CANDLE_LIMIT + 1)).await;

        assert_eq!(result.unwrap_err(), MarketServiceError::InvalidLimit);
    }
}
