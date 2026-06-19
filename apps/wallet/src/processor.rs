use protocol::{
    common::Asset,
    engine::EngineCommand,
    wallet::{
        CancelOrderIntent, CommandAccepted, CommandRejected, InsufficientFunds, PlaceOrderIntent,
        ReleaseReservation, SettleTrade, WalletCommand, WalletDepositApplied, WalletEvent,
        WalletFundsReleased, WalletFundsReserved, WalletReply, WalletTradeSettled,
        WalletWithdrawalApplied,
    },
};
use serde_json::Value;

use crate::{
    repository::{
        AccountDeltaUpdate, WalletRepository, WalletRepositoryError, insufficient_funds_reply,
    },
    router::{WalletAction, route_command},
};

#[derive(Debug, Default)]
pub struct WalletProcessResult {
    pub wallet_replies: Vec<WalletReply>,
    pub wallet_events: Vec<WalletEvent>,
    pub engine_commands: Vec<EngineCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineWalletCommand {
    ReleaseReservation(ReleaseReservation),
    SettleTrade(SettleTrade),
    ApplyAccountDelta(ApplyAccountDelta),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyAccountDelta {
    pub user_id: i64,
    pub asset: Asset,
    pub total_delta: i64,
    pub locked_delta: i64,
    pub kind: String,
    pub reference_id: String,
}

#[derive(Clone)]
pub struct WalletProcessor {
    repository: WalletRepository,
}

impl WalletProcessor {
    pub fn new(repository: WalletRepository) -> Self {
        Self { repository }
    }

    pub async fn process_command(
        &self,
        command: WalletCommand,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        match route_command(command) {
            WalletAction::ReserveAndForward(intent) => self.reserve_and_forward(intent).await,
            WalletAction::ForwardCancel(intent) => self.forward_cancel(intent).await,
            WalletAction::ApplyDeposit(deposit) => {
                let existing = self
                    .repository
                    .get_idempotent_reply(
                        deposit.envelope.user_id,
                        "Deposit",
                        &deposit.envelope.idempotency_key,
                    )
                    .await?;
                if let Some(reply) = existing_reply(existing, &deposit.envelope.request_id)? {
                    return Ok(reply_result(reply));
                }

                let balance = self.repository.apply_deposit(&deposit).await?;
                let reply = WalletReply::BalanceUpdated(balance.clone());

                self.record_reply(
                    deposit.envelope.user_id,
                    "Deposit",
                    &deposit.envelope.idempotency_key,
                    &deposit.envelope.request_id,
                    &reply,
                )
                .await?;

                Ok(WalletProcessResult {
                    wallet_replies: vec![reply],
                    wallet_events: vec![WalletEvent::DepositApplied(WalletDepositApplied {
                        request_id: balance.request_id,
                        user_id: deposit.envelope.user_id,
                        asset: balance.asset,
                        amount: deposit.amount,
                        reference_id: deposit.reference_id,
                        total: balance.total,
                        locked: balance.locked,
                    })],
                    engine_commands: Vec::new(),
                })
            }
            WalletAction::ApplyWithdrawal(withdraw) => {
                let existing = self
                    .repository
                    .get_idempotent_reply(
                        withdraw.envelope.user_id,
                        "Withdraw",
                        &withdraw.envelope.idempotency_key,
                    )
                    .await?;
                if let Some(reply) = existing_reply(existing, &withdraw.envelope.request_id)? {
                    return Ok(reply_result(reply));
                }

                let reply = match self.repository.apply_withdraw(&withdraw).await {
                    Ok(balance) => WalletReply::BalanceUpdated(balance.clone()),
                    Err(WalletRepositoryError::InsufficientFunds { available }) => {
                        WalletReply::InsufficientFunds(InsufficientFunds {
                            request_id: withdraw.envelope.request_id.clone(),
                            asset: withdraw.asset,
                            required: withdraw.amount,
                            available,
                        })
                    }
                    Err(error) => return Err(error),
                };

                self.record_reply(
                    withdraw.envelope.user_id,
                    "Withdraw",
                    &withdraw.envelope.idempotency_key,
                    &withdraw.envelope.request_id,
                    &reply,
                )
                .await?;

                let wallet_events = match &reply {
                    WalletReply::BalanceUpdated(balance) => {
                        vec![WalletEvent::WithdrawalApplied(WalletWithdrawalApplied {
                            request_id: balance.request_id.clone(),
                            user_id: withdraw.envelope.user_id,
                            asset: balance.asset,
                            amount: withdraw.amount,
                            destination: withdraw.destination.clone(),
                            total: balance.total,
                            locked: balance.locked,
                        })]
                    }
                    _ => Vec::new(),
                };

                Ok(WalletProcessResult {
                    wallet_replies: vec![reply],
                    wallet_events,
                    engine_commands: Vec::new(),
                })
            }
            WalletAction::ReleaseReservation(release) => self.release_reservation(release).await,
            WalletAction::SettleTrade(settle) => self.settle_trade(settle).await,
        }
    }

    pub async fn process_engine_command(
        &self,
        command: EngineWalletCommand,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        match command {
            EngineWalletCommand::ReleaseReservation(release) => {
                self.release_reservation(release).await
            }
            EngineWalletCommand::SettleTrade(settle) => self.settle_trade(settle).await,
            EngineWalletCommand::ApplyAccountDelta(delta) => self.apply_account_delta(delta).await,
        }
    }

    async fn reserve_and_forward(
        &self,
        intent: PlaceOrderIntent,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let existing = self
            .repository
            .get_idempotent_reply(
                intent.envelope.user_id,
                "PlaceOrderIntent",
                &intent.envelope.idempotency_key,
            )
            .await?;
        if let Some(reply) = existing_reply(existing, &intent.envelope.request_id)? {
            return Ok(reply_result(reply));
        }

        let reply = match self
            .repository
            .reserve_funds(
                intent.envelope.user_id,
                &intent.envelope.request_id,
                &intent.envelope.idempotency_key,
                intent.margin_asset,
                intent.required_margin,
            )
            .await
        {
            Ok(reserved) => WalletReply::FundsReserved(reserved),
            Err(WalletRepositoryError::InsufficientFunds { available }) => {
                WalletReply::InsufficientFunds(insufficient_funds_reply(
                    intent.envelope.request_id.clone(),
                    intent.margin_asset,
                    intent.required_margin,
                    available,
                ))
            }
            Err(error) => return Err(error),
        };

        self.record_reply(
            intent.envelope.user_id,
            "PlaceOrderIntent",
            &intent.envelope.idempotency_key,
            &intent.envelope.request_id,
            &reply,
        )
        .await?;

        let mut result = WalletProcessResult {
            wallet_replies: vec![reply.clone()],
            wallet_events: Vec::new(),
            engine_commands: Vec::new(),
        };

        if let WalletReply::FundsReserved(reserved) = reply {
            result
                .wallet_events
                .push(WalletEvent::FundsReserved(WalletFundsReserved {
                    request_id: reserved.request_id.clone(),
                    user_id: intent.envelope.user_id,
                    reservation_id: reserved.reservation_id.clone(),
                    asset: reserved.asset,
                    amount: reserved.amount,
                }));
            result.engine_commands.push(EngineCommand::PlaceOrder(
                intent.into_reserved_order(reserved.reservation_id),
            ));
        }

        Ok(result)
    }

    async fn forward_cancel(
        &self,
        intent: CancelOrderIntent,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let existing = self
            .repository
            .get_idempotent_reply(
                intent.envelope.user_id,
                "CancelOrderIntent",
                &intent.envelope.idempotency_key,
            )
            .await?;
        if let Some(reply) = existing_reply(existing, &intent.envelope.request_id)? {
            return Ok(reply_result(reply));
        }

        let reply = WalletReply::CommandAccepted(CommandAccepted {
            request_id: intent.envelope.request_id.clone(),
        });

        self.record_reply(
            intent.envelope.user_id,
            "CancelOrderIntent",
            &intent.envelope.idempotency_key,
            &intent.envelope.request_id,
            &reply,
        )
        .await?;

        Ok(WalletProcessResult {
            wallet_replies: vec![reply],
            wallet_events: Vec::new(),
            engine_commands: vec![EngineCommand::CancelOrder(
                intent.into_engine_cancel_order(),
            )],
        })
    }

    async fn release_reservation(
        &self,
        release: ReleaseReservation,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let reservation = self.repository.release_reservation(&release).await?;

        Ok(WalletProcessResult {
            wallet_replies: Vec::new(),
            wallet_events: vec![WalletEvent::FundsReleased(WalletFundsReleased {
                user_id: reservation.user_id,
                reservation_id: reservation.reservation_id,
                asset: reservation.asset,
                amount: release.amount,
                reason: release.reason,
            })],
            engine_commands: Vec::new(),
        })
    }

    async fn settle_trade(
        &self,
        settle: SettleTrade,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let reservation = self.repository.settle_trade(&settle).await?;

        Ok(WalletProcessResult {
            wallet_replies: Vec::new(),
            wallet_events: vec![WalletEvent::TradeSettled(WalletTradeSettled {
                user_id: reservation.user_id,
                fill_id: settle.fill_id,
                reservation_id: reservation.reservation_id,
                debit_asset: settle.debit_asset,
                debit_amount: settle.debit_amount,
                credit_asset: settle.credit_asset,
                credit_amount: settle.credit_amount,
            })],
            engine_commands: Vec::new(),
        })
    }

    async fn apply_account_delta(
        &self,
        delta: ApplyAccountDelta,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        self.repository
            .apply_account_delta(&AccountDeltaUpdate {
                user_id: delta.user_id,
                asset: delta.asset,
                total_delta: delta.total_delta,
                locked_delta: delta.locked_delta,
                kind: delta.kind,
                reference_id: delta.reference_id,
            })
            .await?;

        Ok(WalletProcessResult::default())
    }

    async fn record_reply(
        &self,
        user_id: i64,
        command_type: &str,
        idempotency_key: &str,
        request_id: &str,
        reply: &WalletReply,
    ) -> Result<(), WalletRepositoryError> {
        self.repository
            .record_idempotent_reply(
                user_id,
                command_type,
                idempotency_key,
                request_id,
                serde_json::to_value(reply)?,
            )
            .await
    }
}

fn existing_reply(
    existing: Option<Value>,
    request_id: &str,
) -> Result<Option<WalletReply>, WalletRepositoryError> {
    existing
        .map(serde_json::from_value)
        .transpose()
        .map_err(WalletRepositoryError::from)
        .map(|reply| reply.map(|reply| reply_with_request_id(reply, request_id)))
}

fn reply_with_request_id(reply: WalletReply, request_id: &str) -> WalletReply {
    match reply {
        WalletReply::FundsReserved(mut reserved) => {
            reserved.request_id = String::from(request_id);
            WalletReply::FundsReserved(reserved)
        }
        WalletReply::InsufficientFunds(mut insufficient) => {
            insufficient.request_id = String::from(request_id);
            WalletReply::InsufficientFunds(insufficient)
        }
        WalletReply::BalanceUpdated(mut balance) => {
            balance.request_id = String::from(request_id);
            WalletReply::BalanceUpdated(balance)
        }
        WalletReply::CommandAccepted(mut accepted) => {
            accepted.request_id = String::from(request_id);
            WalletReply::CommandAccepted(accepted)
        }
        WalletReply::CommandRejected(mut rejected) => {
            rejected.request_id = String::from(request_id);
            WalletReply::CommandRejected(rejected)
        }
    }
}

fn reply_result(reply: WalletReply) -> WalletProcessResult {
    WalletProcessResult {
        wallet_replies: vec![reply],
        wallet_events: Vec::new(),
        engine_commands: Vec::new(),
    }
}

pub fn storage_error_reply(request_id: String, reason: impl Into<String>) -> WalletReply {
    WalletReply::CommandRejected(CommandRejected {
        request_id,
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use protocol::{
        common::{Asset, CommandEnvelope, OrderType, Side},
        wallet::PlaceOrderIntent,
    };

    #[test]
    fn storage_error_reply_contains_request_context() {
        let reply = super::storage_error_reply("req-1".to_string(), "db unavailable");

        match reply {
            protocol::wallet::WalletReply::CommandRejected(rejected) => {
                assert_eq!(rejected.request_id, "req-1");
                assert_eq!(rejected.reason, "db unavailable");
            }
            other => panic!("unexpected reply: {other:?}"),
        }
    }

    #[test]
    fn reserved_order_keeps_original_idempotency_envelope() {
        let intent = PlaceOrderIntent {
            envelope: CommandEnvelope {
                request_id: "req-1".to_string(),
                idempotency_key: "client-order-1".to_string(),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 1,
            market_name: "SOL-PERP".to_string(),
            side: Side::LONG,
            order_type: OrderType::LIMIT,
            quantity: 10,
            price: 20,
            margin_asset: Asset::USDC,
            required_margin: 200,
            leverage: 2,
            reduce_only: true,
        };

        let order = intent.into_reserved_order("res-1".to_string());

        assert_eq!(order.envelope.idempotency_key, "client-order-1");
        assert_eq!(order.reservation_id, "res-1");
        assert!(order.reduce_only);
        assert_eq!(order.leverage, 2);
    }

    #[test]
    fn replayed_idempotent_reply_uses_current_request_id() {
        let reply =
            protocol::wallet::WalletReply::InsufficientFunds(protocol::wallet::InsufficientFunds {
                request_id: "old-req".to_string(),
                asset: Asset::USDC,
                required: 200,
                available: 100,
            });
        let value = serde_json::to_value(reply).expect("reply should serialize");

        let replayed = super::existing_reply(Some(value), "new-req")
            .expect("reply should deserialize")
            .expect("reply should exist");

        match replayed {
            protocol::wallet::WalletReply::InsufficientFunds(insufficient) => {
                assert_eq!(insufficient.request_id, "new-req");
                assert_eq!(insufficient.required, 200);
                assert_eq!(insufficient.available, 100);
            }
            other => panic!("unexpected reply: {other:?}"),
        }
    }
}
