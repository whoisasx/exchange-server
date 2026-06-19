use std::{collections::BTreeSet, error::Error, fmt};

use protocol::{engine::EngineEvent, wallet::WalletEvent};

use crate::{
    hub::Hub,
    messages::{
        EventSource, ServerMessage, StreamMetadata, engine_event_value, wallet_event_value,
    },
};

#[derive(Debug)]
pub enum RouterError {
    Serialization(serde_json::Error),
}

impl From<serde_json::Error> for RouterError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => write!(f, "failed to serialize websocket event: {error}"),
        }
    }
}

impl Error for RouterError {}

#[derive(Clone)]
pub struct EventRouter {
    hub: Hub,
}

impl EventRouter {
    pub fn new(hub: Hub) -> Self {
        Self { hub }
    }

    pub async fn process_engine_event(
        &self,
        event: EngineEvent,
        metadata: StreamMetadata,
    ) -> Result<(), RouterError> {
        let account_user_ids = engine_event_account_user_ids(&event);
        let market_id = engine_event_market_id(&event);

        if account_user_ids.is_empty() && market_id.is_none() {
            return Ok(());
        }

        let event_value = engine_event_value(&event)?;

        for user_id in account_user_ids {
            self.send_account(
                user_id,
                EventSource::Engine,
                event_value.clone(),
                metadata.clone(),
            )
            .await?;
        }

        if let Some(market_id) = market_id {
            self.send_market(market_id, EventSource::Engine, event_value, metadata)
                .await?;
        }

        Ok(())
    }

    pub async fn process_wallet_event(
        &self,
        event: WalletEvent,
        metadata: StreamMetadata,
    ) -> Result<(), RouterError> {
        let event_value = wallet_event_value(&event)?;
        let user_id = wallet_event_user_id(&event);

        self.send_account(user_id, EventSource::Wallet, event_value, metadata)
            .await?;

        Ok(())
    }

    async fn send_account(
        &self,
        user_id: i64,
        source: EventSource,
        event: serde_json::Value,
        metadata: StreamMetadata,
    ) -> Result<(), RouterError> {
        let message = ServerMessage::account_event(source, event, metadata);
        self.hub.broadcast_account(user_id, &message).await?;
        Ok(())
    }

    async fn send_market(
        &self,
        market_id: i64,
        source: EventSource,
        event: serde_json::Value,
        metadata: StreamMetadata,
    ) -> Result<(), RouterError> {
        let message = ServerMessage::market_event(market_id, source, event, metadata);
        self.hub.broadcast_market(market_id, &message).await?;
        Ok(())
    }
}

fn wallet_event_user_id(event: &WalletEvent) -> i64 {
    match event {
        WalletEvent::FundsReserved(event) => event.user_id,
        WalletEvent::FundsReleased(event) => event.user_id,
        WalletEvent::TradeSettled(event) => event.user_id,
        WalletEvent::DepositApplied(event) => event.user_id,
        WalletEvent::WithdrawalApplied(event) => event.user_id,
    }
}

fn engine_event_market_id(event: &EngineEvent) -> Option<i64> {
    match event {
        EngineEvent::OrderOpened(event) => Some(event.market_id),
        EngineEvent::OrderCancelled(event) => Some(event.market_id),
        EngineEvent::OrderExpired(event) => Some(event.market_id),
        EngineEvent::ReservationReleased(event) => Some(event.market_id),
        EngineEvent::TradeExecuted(event) => Some(event.market_id),
        EngineEvent::OrderBookDelta(event) => Some(event.market_id),
        EngineEvent::MarkPriceUpdated(event) => Some(event.market_id),
        EngineEvent::FundingRateUpdated(event) => Some(event.market_id),
        EngineEvent::FundingPaymentApplied(event) => Some(event.market_id),
        EngineEvent::PositionChanged(event) => Some(event.market_id),
        EngineEvent::RiskStateUpdated(event) => Some(event.market_id),
        EngineEvent::FeeCharged(event) => Some(event.market_id),
        EngineEvent::LiquidationStarted(event) => Some(event.market_id),
        EngineEvent::LiquidationExecuted(event) => Some(event.market_id),
        EngineEvent::LiquidationCompleted(event) => Some(event.market_id),
        EngineEvent::AdlExecuted(event) => Some(event.market_id),
        EngineEvent::AccountDelta(event) => Some(event.market_id),
        EngineEvent::OrderBookSnapshotCreated(event) => Some(event.market_id),
        EngineEvent::EngineCheckpointCommitted(_) => None,
    }
}

fn engine_event_account_user_ids(event: &EngineEvent) -> BTreeSet<i64> {
    let mut user_ids = BTreeSet::new();

    match event {
        EngineEvent::OrderOpened(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::OrderCancelled(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::OrderExpired(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::ReservationReleased(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::TradeExecuted(event) => {
            user_ids.extend(trade_users(event.maker_user_id, event.taker_user_id));
            if let Some(user_id) = event.liquidated_user_id {
                user_ids.insert(user_id);
            }
            user_ids.extend(event.fee_deltas.iter().map(|fee| fee.user_id));
        }
        EngineEvent::OrderBookDelta(_) => {}
        EngineEvent::MarkPriceUpdated(_) => {}
        EngineEvent::FundingRateUpdated(_) => {}
        EngineEvent::FundingPaymentApplied(event) => {
            user_ids.extend(event.payments.iter().map(|payment| payment.user_id));
        }
        EngineEvent::PositionChanged(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::RiskStateUpdated(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::FeeCharged(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::LiquidationStarted(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::LiquidationExecuted(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::LiquidationCompleted(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::AdlExecuted(event) => {
            user_ids.insert(event.liquidated_user_id);
            user_ids.insert(event.deleveraged_user_id);
        }
        EngineEvent::AccountDelta(event) => {
            user_ids.insert(event.user_id);
        }
        EngineEvent::OrderBookSnapshotCreated(_) => {}
        EngineEvent::EngineCheckpointCommitted(_) => {}
    }

    user_ids
}

fn trade_users(maker_user_id: i64, taker_user_id: i64) -> BTreeSet<i64> {
    [maker_user_id, taker_user_id].into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trade_users_deduplicate_self_trade() {
        assert_eq!(
            trade_users(42, 42).into_iter().collect::<Vec<_>>(),
            vec![42]
        );
    }

    #[test]
    fn trade_users_are_stable() {
        assert_eq!(
            trade_users(43, 42).into_iter().collect::<Vec<_>>(),
            vec![42, 43]
        );
    }

    #[test]
    fn trade_routing_includes_nested_account_users() {
        let event = engine_event(json!({
            "type": "TradeExecuted",
            "payload": {
                "engine_sequence": 4,
                "engine_timestamp_ms": 1710000003000_i64,
                "fill_id": 7002,
                "market_id": 1,
                "price": 95,
                "quantity": 10,
                "maker_order_id": 9001,
                "taker_order_id": 9003,
                "maker_user_id": 43,
                "taker_user_id": 42,
                "maker_reservation_id": "res_maker_002",
                "taker_reservation_id": "liq_001",
                "liquidated_user_id": 44,
                "fee_deltas": [
                    {
                        "user_id": 45,
                        "asset": "USDC",
                        "amount": 5,
                        "fee_type": "LIQUIDATION"
                    }
                ]
            }
        }));

        assert_eq!(engine_event_market_id(&event), Some(1));
        assert_eq!(
            engine_event_account_user_ids(&event)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![42, 43, 44, 45]
        );
    }

    #[test]
    fn funding_payment_routing_includes_payment_users() {
        let event = engine_event(json!({
            "type": "FundingPaymentApplied",
            "payload": {
                "market_id": 1,
                "engine_sequence": 103,
                "engine_timestamp_ms": 1710028800000_i64,
                "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
                "payments": [
                    {
                        "user_id": 42,
                        "position_id": "pos_42_1",
                        "side": "LONG",
                        "asset": "USDC",
                        "amount": -2
                    },
                    {
                        "user_id": 43,
                        "position_id": "pos_43_1",
                        "side": "SHORT",
                        "asset": "USDC",
                        "amount": 2
                    }
                ]
            }
        }));

        assert_eq!(engine_event_market_id(&event), Some(1));
        assert_eq!(
            engine_event_account_user_ids(&event)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![42, 43]
        );
    }

    #[test]
    fn adl_routing_includes_both_impacted_users() {
        let event = engine_event(json!({
            "type": "AdlExecuted",
            "payload": {
                "market_id": 1,
                "engine_sequence": 113,
                "engine_timestamp_ms": 1710000007200_i64,
                "adl_id": "adl_001",
                "liquidation_id": "liq_002",
                "liquidated_user_id": 42,
                "deleveraged_user_id": 44,
                "position_side": "LONG",
                "quantity": 5,
                "price": 75,
                "rank": 1,
                "reason": "INSURANCE_FUND_INSUFFICIENT"
            }
        }));

        assert_eq!(engine_event_market_id(&event), Some(1));
        assert_eq!(
            engine_event_account_user_ids(&event)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![42, 44]
        );
    }

    #[test]
    fn orderbook_snapshot_is_market_only() {
        let event = engine_event(json!({
            "type": "OrderBookSnapshotCreated",
            "payload": {
                "market_id": 1,
                "engine_sequence": 114,
                "engine_timestamp_ms": 1710000300000_i64,
                "snapshot_id": "orderbook_SOL-PERP_1710000300",
                "uri": "s3://exchange-market-data/orderbooks/market_id=1/1710000300.json.zst",
                "checksum_sha256": "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92",
                "byte_size": 4096,
                "schema_version": 1,
                "last_engine_sequence": 114
            }
        }));

        assert_eq!(engine_event_market_id(&event), Some(1));
        assert!(engine_event_account_user_ids(&event).is_empty());
    }

    #[test]
    fn engine_checkpoint_has_no_live_websocket_route() {
        let event = engine_event(json!({
            "type": "EngineCheckpointCommitted",
            "payload": {
                "checkpoint_id": "engine_checkpoint_1710000300",
                "engine_timestamp_ms": 1710000300000_i64,
                "schema_version": 1,
                "engine_build": "cxx-engine-dev",
                "config_hash": "cfg_001",
                "engine_input_next_offset": 1301,
                "uri": "s3://exchange-engine-checkpoints/checkpoint_1710000300.bin.zst",
                "checksum_sha256": "8843d7f92416211de9ebb963ff4ce28125932878c1f5f96a6b8c7d38f0b4b90f",
                "byte_size": 1048576,
                "market_sequences": [
                    {
                        "market_id": 1,
                        "engine_sequence": 114
                    }
                ]
            }
        }));

        assert_eq!(engine_event_market_id(&event), None);
        assert!(engine_event_account_user_ids(&event).is_empty());
    }

    fn engine_event(value: serde_json::Value) -> EngineEvent {
        serde_json::from_value(value).expect("engine event fixture should parse")
    }
}
