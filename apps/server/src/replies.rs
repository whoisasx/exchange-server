use std::{collections::HashMap, sync::Arc};

use protocol::{engine::EngineReply, wallet::WalletReply};
use serde::Serialize;
use tokio::sync::{Mutex, oneshot};

#[derive(Clone, Default)]
pub struct ReplyState {
    records: Arc<Mutex<HashMap<String, PendingRequest>>>,
}

impl ReplyState {
    pub async fn register_waiter(
        &self,
        request_id: impl Into<String>,
        user_id: i64,
        kind: RequestKind,
    ) -> oneshot::Receiver<ReplyRecord> {
        let request_id = request_id.into();
        let (sender, receiver) = oneshot::channel();
        let mut records = self.records.lock().await;

        records.insert(
            request_id.clone(),
            PendingRequest {
                request_id,
                user_id: Some(user_id),
                kind: Some(kind),
                result: ReplyResult::Pending,
                waiter: Some(sender),
                complete: false,
            },
        );

        receiver
    }

    pub async fn remove(&self, request_id: &str) {
        let mut records = self.records.lock().await;
        records.remove(request_id);
    }

    pub async fn resolve_wallet_reply(&self, reply: WalletReply) {
        let request_id = wallet_reply_request_id(&reply).to_string();
        self.resolve(request_id, ReplyResult::Wallet(reply)).await;
    }

    pub async fn resolve_engine_reply(&self, reply: EngineReply) {
        let request_id = engine_reply_request_id(&reply).to_string();
        self.resolve(request_id, ReplyResult::Engine(reply)).await;
    }

    pub async fn get_for_user(&self, request_id: &str, user_id: i64) -> Option<ReplyRecord> {
        let records = self.records.lock().await;
        records
            .get(request_id)
            .filter(|record| record.user_id == Some(user_id))
            .map(PendingRequest::to_record)
    }

    async fn resolve(&self, request_id: String, result: ReplyResult) {
        let mut records = self.records.lock().await;
        match records.get_mut(&request_id) {
            Some(record) => {
                record.result = result;
                record.complete = is_final_result(record.kind, &record.result);

                if record.complete {
                    let reply = record.to_record();
                    if let Some(waiter) = record.waiter.take() {
                        let _ = waiter.send(reply);
                    }
                }
            }
            None => {
                records.insert(
                    request_id.clone(),
                    PendingRequest {
                        request_id,
                        user_id: None,
                        kind: None,
                        complete: !matches!(result, ReplyResult::Pending),
                        result,
                        waiter: None,
                    },
                );
            }
        }
    }
}

struct PendingRequest {
    request_id: String,
    user_id: Option<i64>,
    kind: Option<RequestKind>,
    result: ReplyResult,
    waiter: Option<oneshot::Sender<ReplyRecord>>,
    complete: bool,
}

impl PendingRequest {
    fn to_record(&self) -> ReplyRecord {
        ReplyRecord {
            request_id: self.request_id.clone(),
            kind: self.kind,
            complete: self.complete,
            result: self.result.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplyRecord {
    pub request_id: String,
    pub kind: Option<RequestKind>,
    pub complete: bool,
    pub result: ReplyResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RequestKind {
    Deposit,
    Withdraw,
    PlaceOrder,
    CancelOrder,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", content = "payload")]
pub enum ReplyResult {
    Pending,
    Wallet(WalletReply),
    Engine(EngineReply),
}

fn is_final_result(kind: Option<RequestKind>, result: &ReplyResult) -> bool {
    match (kind, result) {
        (Some(RequestKind::Deposit), ReplyResult::Wallet(reply)) => matches!(
            reply,
            WalletReply::BalanceUpdated(_) | WalletReply::CommandRejected(_)
        ),
        (Some(RequestKind::Withdraw), ReplyResult::Wallet(reply)) => matches!(
            reply,
            WalletReply::BalanceUpdated(_)
                | WalletReply::InsufficientFunds(_)
                | WalletReply::CommandRejected(_)
        ),
        (Some(RequestKind::PlaceOrder), ReplyResult::Wallet(reply)) => {
            matches!(
                reply,
                WalletReply::InsufficientFunds(_) | WalletReply::CommandRejected(_)
            )
        }
        (Some(RequestKind::PlaceOrder), ReplyResult::Engine(reply)) => {
            matches!(
                reply,
                EngineReply::OrderAccepted(_) | EngineReply::OrderRejected(_)
            )
        }
        (Some(RequestKind::CancelOrder), ReplyResult::Wallet(reply)) => {
            matches!(reply, WalletReply::CommandRejected(_))
        }
        (Some(RequestKind::CancelOrder), ReplyResult::Engine(reply)) => {
            matches!(
                reply,
                EngineReply::CancelAccepted(_) | EngineReply::CancelRejected(_)
            )
        }
        (None, ReplyResult::Pending) => false,
        (None, _) => true,
        _ => false,
    }
}

fn wallet_reply_request_id(reply: &WalletReply) -> &str {
    match reply {
        WalletReply::FundsReserved(reply) => &reply.request_id,
        WalletReply::InsufficientFunds(reply) => &reply.request_id,
        WalletReply::BalanceUpdated(reply) => &reply.request_id,
        WalletReply::CommandAccepted(reply) => &reply.request_id,
        WalletReply::CommandRejected(reply) => &reply.request_id,
    }
}

fn engine_reply_request_id(reply: &EngineReply) -> &str {
    match reply {
        EngineReply::OrderAccepted(reply) => &reply.request_id,
        EngineReply::OrderRejected(reply) => &reply.request_id,
        EngineReply::CancelAccepted(reply) => &reply.request_id,
        EngineReply::CancelRejected(reply) => &reply.request_id,
        EngineReply::LiquidationAccepted(reply) => &reply.request_id,
        EngineReply::LiquidationRejected(reply) => &reply.request_id,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use protocol::{
        common::Asset,
        engine::{EngineReply, OrderAccepted},
        wallet::{BalanceUpdated, FundsReserved, WalletReply},
    };
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn wallet_final_reply_resolves_waiter_for_owner() {
        let state = ReplyState::default();
        let receiver = state
            .register_waiter("req-1", 42, RequestKind::Deposit)
            .await;
        state
            .resolve_wallet_reply(WalletReply::BalanceUpdated(BalanceUpdated {
                request_id: String::from("req-1"),
                asset: Asset::USDC,
                total: 100,
                locked: 0,
            }))
            .await;

        let reply = receiver.await.expect("waiter should resolve");

        assert!(reply.complete);
        assert!(matches!(reply.result, ReplyResult::Wallet(_)));
        assert!(state.get_for_user("req-1", 42).await.is_some());
    }

    #[tokio::test]
    async fn registered_reply_is_hidden_from_other_users() {
        let state = ReplyState::default();
        state
            .register_waiter("req-1", 42, RequestKind::Deposit)
            .await;

        assert!(state.get_for_user("req-1", 7).await.is_none());
    }

    #[tokio::test]
    async fn place_order_waiter_ignores_wallet_funds_reserved() {
        let state = ReplyState::default();
        let receiver = state
            .register_waiter("req-1", 42, RequestKind::PlaceOrder)
            .await;
        state
            .resolve_wallet_reply(WalletReply::FundsReserved(FundsReserved {
                request_id: String::from("req-1"),
                reservation_id: String::from("res-1"),
                asset: Asset::USDC,
                amount: 100,
            }))
            .await;

        assert!(timeout(Duration::from_millis(20), receiver).await.is_err());
        let record = state.get_for_user("req-1", 42).await.unwrap();
        assert!(!record.complete);
        assert!(matches!(record.result, ReplyResult::Wallet(_)));
    }

    #[tokio::test]
    async fn place_order_waiter_resolves_on_engine_reply() {
        let state = ReplyState::default();
        let receiver = state
            .register_waiter("req-1", 42, RequestKind::PlaceOrder)
            .await;
        state
            .resolve_engine_reply(EngineReply::OrderAccepted(OrderAccepted {
                request_id: String::from("req-1"),
                order_id: 99,
                reservation_id: String::from("res-1"),
            }))
            .await;

        let reply = receiver.await.expect("waiter should resolve");

        assert!(reply.complete);
        assert!(matches!(reply.result, ReplyResult::Engine(_)));
    }
}
