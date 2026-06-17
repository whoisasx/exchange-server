# Ledger Service

`apps/ledger` consumes `wallet.events` and writes an immutable audit trail.

Run:

```sh
cargo run -p ledger
```

The wallet remains the hot-path balance owner. Ledger v1 does not replace wallet writes; it mirrors wallet events into:

- `ledger_events`: one journal row per consumed stream record.
- `ledger_entries`: normalized balance deltas derived from each event.
- `ledger_offsets`: consumed Redpanda offsets.

Entry mapping:

| Wallet event | Ledger entries |
| --- | --- |
| `DepositApplied` | `DEPOSIT`: `total_delta=+amount`, `locked_delta=0` |
| `WithdrawalApplied` | `WITHDRAWAL`: `total_delta=-amount`, `locked_delta=0` |
| `FundsReserved` | `RESERVE`: `total_delta=0`, `locked_delta=+amount` |
| `FundsReleased` | `RELEASE`: `total_delta=0`, `locked_delta=-amount` |
| `TradeSettled` | `TRADE_DEBIT`: `total_delta=-debit_amount`, `locked_delta=-debit_amount`; `TRADE_CREDIT`: `total_delta=+credit_amount`, `locked_delta=0` |

Ledger starts from stored offsets, or earliest offsets when no offset is stored.
