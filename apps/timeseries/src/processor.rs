use protocol::engine::TradeExecuted;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
const DAY_MS: i64 = 24 * HOUR_MS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandleInterval {
    pub label: &'static str,
    pub duration_ms: i64,
}

pub const CANDLE_INTERVALS: [CandleInterval; 5] = [
    CandleInterval {
        label: "1m",
        duration_ms: MINUTE_MS,
    },
    CandleInterval {
        label: "5m",
        duration_ms: 5 * MINUTE_MS,
    },
    CandleInterval {
        label: "15m",
        duration_ms: 15 * MINUTE_MS,
    },
    CandleInterval {
        label: "1h",
        duration_ms: HOUR_MS,
    },
    CandleInterval {
        label: "1d",
        duration_ms: DAY_MS,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandleDraft {
    pub market_id: i64,
    pub interval: &'static str,
    pub bucket_start_ms: i64,
    pub price: i64,
    pub quantity: i64,
    pub engine_sequence: i64,
}

#[derive(Clone, Default)]
pub struct TimeseriesProcessor;

impl TimeseriesProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn candle_drafts(&self, trade: &TradeExecuted) -> Vec<CandleDraft> {
        candle_drafts_from_trade(trade)
    }
}

pub fn candle_drafts_from_trade(trade: &TradeExecuted) -> Vec<CandleDraft> {
    CANDLE_INTERVALS
        .iter()
        .map(|interval| CandleDraft {
            market_id: trade.market_id,
            interval: interval.label,
            bucket_start_ms: bucket_start_ms(trade.engine_timestamp_ms, interval.duration_ms),
            price: trade.price,
            quantity: trade.quantity,
            engine_sequence: trade.engine_sequence,
        })
        .collect()
}

pub fn bucket_start_ms(timestamp_ms: i64, interval_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(interval_ms)
}

#[cfg(test)]
mod tests {
    use protocol::engine::{ExecutionReason, TradeExecuted};

    use super::*;

    #[test]
    fn bucket_start_floors_to_interval() {
        assert_eq!(bucket_start_ms(1710000123456, MINUTE_MS), 1710000120000);
        assert_eq!(bucket_start_ms(1710000123456, 5 * MINUTE_MS), 1710000000000);
        assert_eq!(bucket_start_ms(1710000123456, HOUR_MS), 1710000000000);
    }

    #[test]
    fn candle_drafts_include_core_intervals() {
        let trade = TradeExecuted {
            engine_sequence: 12,
            engine_timestamp_ms: 1710000123456,
            fill_id: 1,
            market_id: 7,
            price: 100,
            quantity: 5,
            maker_order_id: 10,
            taker_order_id: 11,
            maker_user_id: 42,
            taker_user_id: 43,
            maker_reservation_id: None,
            taker_reservation_id: None,
            execution_reason: ExecutionReason::TRADE,
            settlements: Vec::new(),
        };

        let drafts = candle_drafts_from_trade(&trade);
        let intervals = drafts
            .iter()
            .map(|draft| draft.interval)
            .collect::<Vec<_>>();

        assert_eq!(intervals, vec!["1m", "5m", "15m", "1h", "1d"]);
        assert!(drafts.iter().all(|draft| draft.market_id == 7));
        assert!(drafts.iter().all(|draft| draft.engine_sequence == 12));
    }
}
