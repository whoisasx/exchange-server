use protocol::{
    common::Asset,
    wallet::{
        WalletAccountDeltaApplied, WalletDepositApplied, WalletEvent, WalletFundsReleased,
        WalletFundsReserved, WalletTradeSettled, WalletWithdrawalApplied,
    },
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRecord {
    pub logical_event_id: Option<String>,
    pub event_type: &'static str,
    pub user_id: i64,
    pub payload: Value,
    pub entries: Vec<LedgerEntryDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntryDraft {
    pub user_id: i64,
    pub asset: Asset,
    pub kind: String,
    pub total_delta: i64,
    pub locked_delta: i64,
    pub reference_id: String,
}

#[derive(Clone, Default)]
pub struct LedgerProcessor;

impl LedgerProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_wallet_event(
        &self,
        event: &WalletEvent,
    ) -> Result<LedgerRecord, serde_json::Error> {
        ledger_record_from_wallet_event(event)
    }
}

pub fn ledger_record_from_wallet_event(
    event: &WalletEvent,
) -> Result<LedgerRecord, serde_json::Error> {
    let payload = serde_json::to_value(event)?;
    let logical_event_id = event.event_id().map(String::from);

    let (event_type, user_id, entries) = match event {
        WalletEvent::FundsReserved(event) => funds_reserved_entries(event),
        WalletEvent::FundsReleased(event) => funds_released_entries(event),
        WalletEvent::TradeSettled(event) => trade_settled_entries(event),
        WalletEvent::DepositApplied(event) => deposit_entries(event),
        WalletEvent::WithdrawalApplied(event) => withdrawal_entries(event),
        WalletEvent::AccountDeltaApplied(event) => account_delta_entries(event),
    };

    Ok(LedgerRecord {
        logical_event_id,
        event_type,
        user_id,
        payload,
        entries,
    })
}

fn funds_reserved_entries(
    event: &WalletFundsReserved,
) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    (
        "FundsReserved",
        event.user_id,
        vec![LedgerEntryDraft {
            user_id: event.user_id,
            asset: event.asset,
            kind: String::from("RESERVE"),
            total_delta: 0,
            locked_delta: event.amount,
            reference_id: event.reservation_id.clone(),
        }],
    )
}

fn funds_released_entries(
    event: &WalletFundsReleased,
) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    (
        "FundsReleased",
        event.user_id,
        vec![LedgerEntryDraft {
            user_id: event.user_id,
            asset: event.asset,
            kind: String::from("RELEASE"),
            total_delta: 0,
            locked_delta: -event.amount,
            reference_id: event.reservation_id.clone(),
        }],
    )
}

fn trade_settled_entries(event: &WalletTradeSettled) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    let reference_id = event.fill_id.to_string();

    (
        "TradeSettled",
        event.user_id,
        vec![
            LedgerEntryDraft {
                user_id: event.user_id,
                asset: event.debit_asset,
                kind: String::from("TRADE_DEBIT"),
                total_delta: -event.debit_amount,
                locked_delta: -event.debit_amount,
                reference_id: reference_id.clone(),
            },
            LedgerEntryDraft {
                user_id: event.user_id,
                asset: event.credit_asset,
                kind: String::from("TRADE_CREDIT"),
                total_delta: event.credit_amount,
                locked_delta: 0,
                reference_id,
            },
        ],
    )
}

fn deposit_entries(event: &WalletDepositApplied) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    (
        "DepositApplied",
        event.user_id,
        vec![LedgerEntryDraft {
            user_id: event.user_id,
            asset: event.asset,
            kind: String::from("DEPOSIT"),
            total_delta: event.amount,
            locked_delta: 0,
            reference_id: event.reference_id.clone(),
        }],
    )
}

fn withdrawal_entries(
    event: &WalletWithdrawalApplied,
) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    (
        "WithdrawalApplied",
        event.user_id,
        vec![LedgerEntryDraft {
            user_id: event.user_id,
            asset: event.asset,
            kind: String::from("WITHDRAWAL"),
            total_delta: -event.amount,
            locked_delta: 0,
            reference_id: event.request_id.clone(),
        }],
    )
}

fn account_delta_entries(
    event: &WalletAccountDeltaApplied,
) -> (&'static str, i64, Vec<LedgerEntryDraft>) {
    (
        "AccountDeltaApplied",
        event.user_id,
        vec![LedgerEntryDraft {
            user_id: event.user_id,
            asset: event.asset,
            kind: event.kind.clone(),
            total_delta: event.total_delta,
            locked_delta: event.locked_delta,
            reference_id: event.reference_id.clone(),
        }],
    )
}

#[cfg(test)]
mod tests {
    use protocol::{
        common::Asset,
        wallet::{
            WalletAccountDeltaApplied, WalletDepositApplied, WalletEvent, WalletFundsReleased,
            WalletFundsReserved, WalletTradeSettled, WalletWithdrawalApplied,
        },
    };

    use super::*;

    #[test]
    fn deposit_event_maps_to_total_increase() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::DepositApplied(WalletDepositApplied {
                event_id: Some(String::from("wallet-event:deposit-applied:42:deposit-1")),
                request_id: String::from("req-1"),
                user_id: 42,
                asset: Asset::USDC,
                amount: 100,
                reference_id: String::from("deposit-1"),
                total: 100,
                locked: 0,
            }))
            .expect("deposit should map");

        assert_eq!(record.event_type, "DepositApplied");
        assert_eq!(
            record.logical_event_id.as_deref(),
            Some("wallet-event:deposit-applied:42:deposit-1")
        );
        assert_eq!(record.user_id, 42);
        assert_eq!(record.entries[0].kind, "DEPOSIT");
        assert_eq!(record.entries[0].total_delta, 100);
        assert_eq!(record.entries[0].locked_delta, 0);
        assert_eq!(record.entries[0].reference_id, "deposit-1");
    }

    #[test]
    fn withdrawal_event_maps_to_total_decrease() {
        let record = ledger_record_from_wallet_event(&WalletEvent::WithdrawalApplied(
            WalletWithdrawalApplied {
                event_id: None,
                request_id: String::from("req-2"),
                user_id: 42,
                asset: Asset::USDC,
                amount: 50,
                destination: String::from("bank"),
                total: 50,
                locked: 0,
            },
        ))
        .expect("withdrawal should map");

        assert_eq!(record.event_type, "WithdrawalApplied");
        assert_eq!(record.entries[0].kind, "WITHDRAWAL");
        assert_eq!(record.entries[0].total_delta, -50);
        assert_eq!(record.entries[0].reference_id, "req-2");
    }

    #[test]
    fn reservation_event_maps_to_locked_increase() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::FundsReserved(WalletFundsReserved {
                event_id: None,
                request_id: String::from("req-3"),
                user_id: 42,
                reservation_id: String::from("res-1"),
                asset: Asset::USDC,
                amount: 70,
            }))
            .expect("reservation should map");

        assert_eq!(record.entries[0].kind, "RESERVE");
        assert_eq!(record.entries[0].total_delta, 0);
        assert_eq!(record.entries[0].locked_delta, 70);
        assert_eq!(record.entries[0].reference_id, "res-1");
    }

    #[test]
    fn release_event_maps_to_locked_decrease() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::FundsReleased(WalletFundsReleased {
                event_id: None,
                user_id: 42,
                reservation_id: String::from("res-1"),
                asset: Asset::USDC,
                amount: 70,
                reason: String::from("cancel"),
            }))
            .expect("release should map");

        assert_eq!(record.entries[0].kind, "RELEASE");
        assert_eq!(record.entries[0].locked_delta, -70);
    }

    #[test]
    fn trade_event_maps_to_debit_and_credit_entries() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::TradeSettled(WalletTradeSettled {
                event_id: None,
                user_id: 42,
                fill_id: 7,
                reservation_id: String::from("res-1"),
                debit_asset: Asset::USDC,
                debit_amount: 100,
                credit_asset: Asset::SOL,
                credit_amount: 10,
            }))
            .expect("trade should map");

        assert_eq!(record.entries.len(), 2);
        assert_eq!(record.entries[0].kind, "TRADE_DEBIT");
        assert_eq!(record.entries[0].total_delta, -100);
        assert_eq!(record.entries[0].locked_delta, -100);
        assert_eq!(record.entries[1].kind, "TRADE_CREDIT");
        assert_eq!(record.entries[1].total_delta, 10);
        assert_eq!(record.entries[1].locked_delta, 0);
    }

    #[test]
    fn same_asset_trade_event_reports_locked_decrease() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::TradeSettled(WalletTradeSettled {
                event_id: None,
                user_id: 42,
                fill_id: 7,
                reservation_id: String::from("res-1"),
                debit_asset: Asset::USDC,
                debit_amount: 100,
                credit_asset: Asset::USDC,
                credit_amount: 100,
            }))
            .expect("trade should map");

        assert_eq!(record.entries.len(), 2);
        assert_eq!(record.entries[0].kind, "TRADE_DEBIT");
        assert_eq!(record.entries[0].total_delta, -100);
        assert_eq!(record.entries[0].locked_delta, -100);
        assert_eq!(record.entries[1].kind, "TRADE_CREDIT");
        assert_eq!(record.entries[1].total_delta, 100);
        assert_eq!(record.entries[1].locked_delta, 0);
    }

    #[test]
    fn same_asset_unequal_trade_event_keeps_spot_like_locked_decrease() {
        let record =
            ledger_record_from_wallet_event(&WalletEvent::TradeSettled(WalletTradeSettled {
                event_id: None,
                user_id: 42,
                fill_id: 7,
                reservation_id: String::from("res-1"),
                debit_asset: Asset::USDC,
                debit_amount: 100,
                credit_asset: Asset::USDC,
                credit_amount: 99,
            }))
            .expect("trade should map");

        assert_eq!(record.entries[0].kind, "TRADE_DEBIT");
        assert_eq!(record.entries[0].locked_delta, -100);
    }

    #[test]
    fn account_delta_event_maps_to_dynamic_kind_entry() {
        let record = ledger_record_from_wallet_event(&WalletEvent::AccountDeltaApplied(
            WalletAccountDeltaApplied {
                event_id: None,
                user_id: 42,
                asset: Asset::USDC,
                total_delta: -3,
                locked_delta: 0,
                kind: String::from("TRADE_FEE"),
                reference_id: String::from("fill:7:fee:0:42:TAKER"),
                total: 997,
                locked: 0,
            },
        ))
        .expect("account delta should map");

        assert_eq!(record.event_type, "AccountDeltaApplied");
        assert_eq!(record.user_id, 42);
        assert_eq!(record.entries[0].kind, "TRADE_FEE");
        assert_eq!(record.entries[0].total_delta, -3);
        assert_eq!(record.entries[0].locked_delta, 0);
        assert_eq!(record.entries[0].reference_id, "fill:7:fee:0:42:TAKER");
    }
}
