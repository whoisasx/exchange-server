use protocol::{
    common::Asset,
    engine::{self, EngineCommand},
    wallet::{
        self, BalanceUpdated, CancelOrderIntent, CommandAccepted, CommandRejected, Deposit,
        FundsReserved, InsufficientFunds, PlaceOrderIntent, ReleaseReservation, SettleTrade,
        WalletAccountDeltaApplied, WalletCommand, WalletDepositApplied, WalletEvent,
        WalletFundsReleased, WalletFundsReserved, WalletReply, WalletTradeSettled,
        WalletWithdrawalApplied, Withdraw,
    },
};
use serde_json::Value;

use crate::{
    engine_inputs::engine_command_outbox_message,
    repository::{
        AccountDeltaUpdate, BalanceSnapshot, NewWalletOutboxMessage, ReservationRecord,
        WalletRepository, WalletRepositoryError, insufficient_funds_reply,
    },
    router::{WalletAction, route_command},
};

#[derive(Debug, Default)]
pub struct WalletProcessResult {
    pub wallet_replies: Vec<WalletReply>,
    pub wallet_events: Vec<WalletEvent>,
    pub engine_inputs: Vec<EngineCommand>,
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
    wallet_events_topic: String,
    engine_input_topic: String,
}

impl WalletProcessor {
    pub fn new(repository: WalletRepository) -> Self {
        Self::new_with_topics(
            repository,
            wallet::WALLET_EVENTS_TOPIC,
            engine::ENGINE_INPUT_TOPIC,
        )
    }

    pub fn new_with_topics(
        repository: WalletRepository,
        wallet_events_topic: impl Into<String>,
        engine_input_topic: impl Into<String>,
    ) -> Self {
        Self {
            repository,
            wallet_events_topic: wallet_events_topic.into(),
            engine_input_topic: engine_input_topic.into(),
        }
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

                let wallet_events_topic = self.wallet_events_topic.clone();
                let wallet_key = deposit.envelope.user_id.to_string();
                let dedupe_key = format!(
                    "wallet-event:deposit-applied:{}:{}",
                    deposit.envelope.user_id, deposit.envelope.idempotency_key
                );
                let balance = self
                    .repository
                    .apply_deposit_with_outbox(&deposit, "Deposit", |balance| {
                        let reply = WalletReply::BalanceUpdated(balance.clone());
                        let event = deposit_applied_event(&dedupe_key, &deposit, balance);
                        let outbox = wallet_event_outbox_message(
                            &wallet_events_topic,
                            dedupe_key.clone(),
                            wallet_key.clone(),
                            &event,
                        )?;

                        Ok((serde_json::to_value(&reply)?, vec![outbox]))
                    })
                    .await?;
                let reply = WalletReply::BalanceUpdated(balance.clone());
                let event = deposit_applied_event(&dedupe_key, &deposit, &balance);

                Ok(WalletProcessResult {
                    wallet_replies: vec![reply],
                    wallet_events: vec![event],
                    engine_inputs: Vec::new(),
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

                let wallet_events_topic = self.wallet_events_topic.clone();
                let wallet_key = withdraw.envelope.user_id.to_string();
                let dedupe_key = format!(
                    "wallet-event:withdrawal-applied:{}:{}",
                    withdraw.envelope.user_id, withdraw.envelope.idempotency_key
                );
                let balance = match self
                    .repository
                    .apply_withdraw_with_outbox(&withdraw, "Withdraw", |balance| {
                        let reply = WalletReply::BalanceUpdated(balance.clone());
                        let event = withdrawal_applied_event(&dedupe_key, &withdraw, balance);
                        let outbox = wallet_event_outbox_message(
                            &wallet_events_topic,
                            dedupe_key.clone(),
                            wallet_key.clone(),
                            &event,
                        )?;

                        Ok((serde_json::to_value(&reply)?, vec![outbox]))
                    })
                    .await
                {
                    Ok(balance) => balance,
                    Err(WalletRepositoryError::InsufficientFunds { available }) => {
                        let reply = WalletReply::InsufficientFunds(InsufficientFunds {
                            request_id: withdraw.envelope.request_id.clone(),
                            asset: withdraw.asset,
                            required: withdraw.amount,
                            available,
                        });
                        self.record_reply(
                            withdraw.envelope.user_id,
                            "Withdraw",
                            &withdraw.envelope.idempotency_key,
                            &withdraw.envelope.request_id,
                            &reply,
                        )
                        .await?;

                        return Ok(WalletProcessResult {
                            wallet_replies: vec![reply],
                            wallet_events: Vec::new(),
                            engine_inputs: Vec::new(),
                        });
                    }
                    Err(error) => return Err(error),
                };
                let reply = WalletReply::BalanceUpdated(balance.clone());
                let event = withdrawal_applied_event(&dedupe_key, &withdraw, &balance);

                Ok(WalletProcessResult {
                    wallet_replies: vec![reply],
                    wallet_events: vec![event],
                    engine_inputs: Vec::new(),
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

        let wallet_events_topic = self.wallet_events_topic.clone();
        let engine_input_topic = self.engine_input_topic.clone();
        let wallet_key = intent.envelope.user_id.to_string();
        let engine_dedupe_key = format!(
            "engine-input:place-order:{}:{}",
            intent.envelope.user_id, intent.envelope.idempotency_key
        );

        let reserved = match self
            .repository
            .reserve_funds_with_outbox(
                intent.envelope.user_id,
                &intent.envelope.request_id,
                &intent.envelope.idempotency_key,
                intent.margin_asset,
                intent.required_margin,
                "PlaceOrderIntent",
                |reserved| {
                    let reply = WalletReply::FundsReserved(reserved.clone());
                    let wallet_event_id =
                        format!("wallet-event:funds-reserved:{}", reserved.reservation_id);
                    let event = funds_reserved_event(&wallet_event_id, &intent, reserved);
                    let engine_command = EngineCommand::PlaceOrder(
                        intent
                            .clone()
                            .into_reserved_order(reserved.reservation_id.clone()),
                    );
                    let wallet_outbox = wallet_event_outbox_message(
                        &wallet_events_topic,
                        wallet_event_id,
                        wallet_key.clone(),
                        &event,
                    )?;
                    let engine_outbox = engine_command_outbox_message(
                        &engine_input_topic,
                        engine_dedupe_key.clone(),
                        &engine_command,
                    )?;

                    Ok((
                        serde_json::to_value(&reply)?,
                        vec![wallet_outbox, engine_outbox],
                    ))
                },
            )
            .await
        {
            Ok(reserved) => reserved,
            Err(WalletRepositoryError::InsufficientFunds { available }) => {
                let reply = WalletReply::InsufficientFunds(insufficient_funds_reply(
                    intent.envelope.request_id.clone(),
                    intent.margin_asset,
                    intent.required_margin,
                    available,
                ));
                self.record_reply(
                    intent.envelope.user_id,
                    "PlaceOrderIntent",
                    &intent.envelope.idempotency_key,
                    &intent.envelope.request_id,
                    &reply,
                )
                .await?;

                return Ok(WalletProcessResult {
                    wallet_replies: vec![reply],
                    wallet_events: Vec::new(),
                    engine_inputs: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        let reply = WalletReply::FundsReserved(reserved.clone());
        let event = funds_reserved_event(
            &format!("wallet-event:funds-reserved:{}", reserved.reservation_id),
            &intent,
            &reserved,
        );
        let engine_command =
            EngineCommand::PlaceOrder(intent.into_reserved_order(reserved.reservation_id.clone()));

        Ok(WalletProcessResult {
            wallet_replies: vec![reply],
            wallet_events: vec![event],
            engine_inputs: vec![engine_command],
        })
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
        let engine_command = EngineCommand::CancelOrder(intent.clone().into_engine_cancel_order());
        let engine_outbox = engine_command_outbox_message(
            &self.engine_input_topic,
            format!(
                "engine-input:cancel-order:{}:{}",
                intent.envelope.user_id, intent.envelope.idempotency_key
            ),
            &engine_command,
        )?;

        self.repository
            .record_idempotent_reply_with_outbox(
                intent.envelope.user_id,
                "CancelOrderIntent",
                &intent.envelope.idempotency_key,
                &intent.envelope.request_id,
                serde_json::to_value(&reply)?,
                &[engine_outbox],
            )
            .await?;

        Ok(WalletProcessResult {
            wallet_replies: vec![reply],
            wallet_events: Vec::new(),
            engine_inputs: vec![engine_command],
        })
    }

    async fn release_reservation(
        &self,
        release: ReleaseReservation,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let wallet_events_topic = self.wallet_events_topic.clone();
        let dedupe_key = format!(
            "wallet-event:funds-released:{}:{}:{}",
            release.reservation_id, release.reason, release.amount
        );
        let reservation = self
            .repository
            .release_reservation_with_outbox(&release, |reservation| {
                let event = funds_released_event(&dedupe_key, &release, reservation);
                let outbox = wallet_event_outbox_message(
                    &wallet_events_topic,
                    dedupe_key.clone(),
                    reservation.reservation_id.clone(),
                    &event,
                )?;

                Ok(vec![outbox])
            })
            .await?;
        let event = funds_released_event(&dedupe_key, &release, &reservation);

        Ok(WalletProcessResult {
            wallet_replies: Vec::new(),
            wallet_events: vec![event],
            engine_inputs: Vec::new(),
        })
    }

    async fn settle_trade(
        &self,
        settle: SettleTrade,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let wallet_events_topic = self.wallet_events_topic.clone();
        let dedupe_key = format!(
            "wallet-event:trade-settled:{}:{}",
            settle.fill_id, settle.reservation_id
        );
        let reservation = self
            .repository
            .settle_trade_with_outbox(&settle, |reservation| {
                let event = trade_settled_event(&dedupe_key, &settle, reservation);
                let outbox = wallet_event_outbox_message(
                    &wallet_events_topic,
                    dedupe_key.clone(),
                    reservation.reservation_id.clone(),
                    &event,
                )?;

                Ok(vec![outbox])
            })
            .await?;
        let event = trade_settled_event(&dedupe_key, &settle, &reservation);

        Ok(WalletProcessResult {
            wallet_replies: Vec::new(),
            wallet_events: vec![event],
            engine_inputs: Vec::new(),
        })
    }

    async fn apply_account_delta(
        &self,
        delta: ApplyAccountDelta,
    ) -> Result<WalletProcessResult, WalletRepositoryError> {
        let update = AccountDeltaUpdate {
            user_id: delta.user_id,
            asset: delta.asset,
            total_delta: delta.total_delta,
            locked_delta: delta.locked_delta,
            kind: delta.kind.clone(),
            reference_id: delta.reference_id.clone(),
        };
        let wallet_events_topic = self.wallet_events_topic.clone();
        let dedupe_key = format!(
            "wallet-event:account-delta-applied:{}:{}:{}:{:?}",
            delta.kind, delta.reference_id, delta.user_id, delta.asset
        );
        let balance = self
            .repository
            .apply_account_delta_with_outbox(&update, |balance| {
                let event = account_delta_applied_event(&dedupe_key, &delta, balance);
                let outbox = wallet_event_outbox_message(
                    &wallet_events_topic,
                    dedupe_key.clone(),
                    delta.user_id.to_string(),
                    &event,
                )?;

                Ok(vec![outbox])
            })
            .await?;

        let Some(balance) = balance else {
            return Ok(WalletProcessResult::default());
        };
        let event = account_delta_applied_event(&dedupe_key, &delta, &balance);

        Ok(WalletProcessResult {
            wallet_replies: Vec::new(),
            wallet_events: vec![event],
            engine_inputs: Vec::new(),
        })
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
        engine_inputs: Vec::new(),
    }
}

fn funds_reserved_event(
    event_id: &str,
    intent: &PlaceOrderIntent,
    reserved: &FundsReserved,
) -> WalletEvent {
    WalletEvent::FundsReserved(WalletFundsReserved {
        event_id: Some(String::from(event_id)),
        request_id: reserved.request_id.clone(),
        user_id: intent.envelope.user_id,
        reservation_id: reserved.reservation_id.clone(),
        asset: reserved.asset,
        amount: reserved.amount,
    })
}

fn deposit_applied_event(
    event_id: &str,
    deposit: &Deposit,
    balance: &BalanceUpdated,
) -> WalletEvent {
    WalletEvent::DepositApplied(WalletDepositApplied {
        event_id: Some(String::from(event_id)),
        request_id: balance.request_id.clone(),
        user_id: deposit.envelope.user_id,
        asset: balance.asset,
        amount: deposit.amount,
        reference_id: deposit.reference_id.clone(),
        total: balance.total,
        locked: balance.locked,
    })
}

fn withdrawal_applied_event(
    event_id: &str,
    withdraw: &Withdraw,
    balance: &BalanceUpdated,
) -> WalletEvent {
    WalletEvent::WithdrawalApplied(WalletWithdrawalApplied {
        event_id: Some(String::from(event_id)),
        request_id: balance.request_id.clone(),
        user_id: withdraw.envelope.user_id,
        asset: balance.asset,
        amount: withdraw.amount,
        destination: withdraw.destination.clone(),
        total: balance.total,
        locked: balance.locked,
    })
}

fn funds_released_event(
    event_id: &str,
    release: &ReleaseReservation,
    reservation: &ReservationRecord,
) -> WalletEvent {
    WalletEvent::FundsReleased(WalletFundsReleased {
        event_id: Some(String::from(event_id)),
        user_id: reservation.user_id,
        reservation_id: reservation.reservation_id.clone(),
        asset: reservation.asset,
        amount: release.amount,
        reason: release.reason.clone(),
    })
}

fn trade_settled_event(
    event_id: &str,
    settle: &SettleTrade,
    reservation: &ReservationRecord,
) -> WalletEvent {
    WalletEvent::TradeSettled(WalletTradeSettled {
        event_id: Some(String::from(event_id)),
        user_id: reservation.user_id,
        fill_id: settle.fill_id,
        reservation_id: reservation.reservation_id.clone(),
        debit_asset: settle.debit_asset,
        debit_amount: settle.debit_amount,
        credit_asset: settle.credit_asset,
        credit_amount: settle.credit_amount,
    })
}

fn account_delta_applied_event(
    event_id: &str,
    delta: &ApplyAccountDelta,
    balance: &BalanceSnapshot,
) -> WalletEvent {
    WalletEvent::AccountDeltaApplied(WalletAccountDeltaApplied {
        event_id: Some(String::from(event_id)),
        user_id: delta.user_id,
        asset: delta.asset,
        total_delta: delta.total_delta,
        locked_delta: delta.locked_delta,
        kind: delta.kind.clone(),
        reference_id: delta.reference_id.clone(),
        total: balance.total,
        locked: balance.locked,
    })
}

fn wallet_event_outbox_message(
    topic: &str,
    dedupe_key: String,
    message_key: String,
    event: &WalletEvent,
) -> Result<NewWalletOutboxMessage, WalletRepositoryError> {
    NewWalletOutboxMessage::json(dedupe_key, topic, None, message_key, "WalletEvent", event)
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
            order_id: 99,
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
        assert_eq!(order.order_id, 99);
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
