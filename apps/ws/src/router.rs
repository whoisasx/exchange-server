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
        let event_value = engine_event_value(&event)?;

        match &event {
            EngineEvent::OrderOpened(opened) => {
                self.send_account(
                    opened.user_id,
                    EventSource::Engine,
                    event_value.clone(),
                    metadata.clone(),
                )
                .await?;
                self.send_market(opened.market_id, EventSource::Engine, event_value, metadata)
                    .await?;
            }
            EngineEvent::OrderCancelled(cancelled) => {
                self.send_account(
                    cancelled.user_id,
                    EventSource::Engine,
                    event_value.clone(),
                    metadata.clone(),
                )
                .await?;
                self.send_market(
                    cancelled.market_id,
                    EventSource::Engine,
                    event_value,
                    metadata,
                )
                .await?;
            }
            EngineEvent::TradeExecuted(trade) => {
                self.send_market(
                    trade.market_id,
                    EventSource::Engine,
                    event_value.clone(),
                    metadata.clone(),
                )
                .await?;

                for user_id in trade_users(trade.maker_user_id, trade.taker_user_id) {
                    self.send_account(
                        user_id,
                        EventSource::Engine,
                        event_value.clone(),
                        metadata.clone(),
                    )
                    .await?;
                }
            }
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

fn trade_users(maker_user_id: i64, taker_user_id: i64) -> BTreeSet<i64> {
    [maker_user_id, taker_user_id].into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
