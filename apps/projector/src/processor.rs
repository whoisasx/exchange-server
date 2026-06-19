use protocol::{
    engine::{EngineEvent, EngineInput, EngineReply},
    wallet::WalletEvent,
};

use crate::repository::{ProjectorRepository, ProjectorRepositoryError};

#[derive(Clone)]
pub struct ProjectorProcessor {
    repository: ProjectorRepository,
}

impl ProjectorProcessor {
    pub fn new(repository: ProjectorRepository) -> Self {
        Self { repository }
    }

    pub async fn process_engine_input(
        &self,
        input: EngineInput,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        match input {
            EngineInput::PlaceOrder(order) => {
                self.repository
                    .save_order_context(&order, topic, partition, next_offset)
                    .await
            }
            EngineInput::LiquidatePosition(liquidation) => {
                self.repository
                    .save_liquidation_context(&liquidation, topic, partition, next_offset)
                    .await
            }
            EngineInput::CancelOrder(_)
            | EngineInput::MarkPriceUpdated(_)
            | EngineInput::FundingRateUpdated(_)
            | EngineInput::FundingSettlementTick(_) => {
                self.repository
                    .save_queue_offset(topic, partition, next_offset)
                    .await
            }
        }
    }

    pub async fn process_engine_reply(
        &self,
        reply: EngineReply,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        match reply {
            EngineReply::OrderAccepted(reply) => {
                self.repository
                    .mark_order_accepted(&reply, topic, partition, next_offset)
                    .await
            }
            EngineReply::OrderRejected(reply) => {
                self.repository
                    .mark_order_rejected(&reply, topic, partition, next_offset)
                    .await
            }
            EngineReply::LiquidationAccepted(reply) => {
                self.repository
                    .mark_liquidation_accepted(&reply, topic, partition, next_offset)
                    .await
            }
            EngineReply::LiquidationRejected(reply) => {
                self.repository
                    .mark_liquidation_rejected(&reply, topic, partition, next_offset)
                    .await
            }
            EngineReply::CancelAccepted(_) | EngineReply::CancelRejected(_) => {
                self.repository
                    .save_queue_offset(topic, partition, next_offset)
                    .await
            }
        }
    }

    pub async fn process_engine_event(
        &self,
        event: EngineEvent,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        match event {
            EngineEvent::OrderOpened(event) => {
                self.repository
                    .project_order_opened(&event, topic, partition, next_offset)
                    .await
            }
            EngineEvent::OrderCancelled(event) => {
                self.repository
                    .project_order_cancelled(&event, topic, partition, next_offset)
                    .await
            }
            EngineEvent::TradeExecuted(event) => {
                self.repository
                    .project_trade_executed(&event, topic, partition, next_offset)
                    .await
            }
            EngineEvent::OrderBookDelta(event) => {
                self.repository
                    .project_orderbook_delta(&event, topic, partition, next_offset)
                    .await
            }
            event @ (EngineEvent::OrderExpired(_)
            | EngineEvent::ReservationReleased(_)
            | EngineEvent::MarkPriceUpdated(_)
            | EngineEvent::FundingRateUpdated(_)
            | EngineEvent::FundingPaymentApplied(_)
            | EngineEvent::PositionChanged(_)
            | EngineEvent::RiskStateUpdated(_)
            | EngineEvent::FeeCharged(_)
            | EngineEvent::LiquidationStarted(_)
            | EngineEvent::LiquidationExecuted(_)
            | EngineEvent::LiquidationCompleted(_)
            | EngineEvent::AdlExecuted(_)
            | EngineEvent::AccountDelta(_)
            | EngineEvent::OrderBookSnapshotCreated(_)
            | EngineEvent::EngineCheckpointCommitted(_)) => {
                self.repository
                    .project_engine_event_log(&event, topic, partition, next_offset)
                    .await
            }
        }
    }

    pub async fn process_wallet_event(
        &self,
        _event: WalletEvent,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        self.repository
            .save_queue_offset(topic, partition, next_offset)
            .await
    }
}
