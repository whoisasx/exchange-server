use db::dto::AssetType;
use protocol::{
    common::Asset,
    wallet::{
        BalanceUpdated, Deposit, FundsReserved, InsufficientFunds, ReleaseReservation, SettleTrade,
        Withdraw,
    },
};
use serde::Serialize;
use serde_json::Value;
use sqlx::{Pool, Postgres, Row, Transaction};
use uuid::Uuid;

const SAVE_QUEUE_OFFSET_SQL: &str = r#"
INSERT INTO wallet_queue_offsets(topic, partition, next_offset)
VALUES($1,$2,$3)
ON CONFLICT(topic, partition)
DO UPDATE
SET next_offset=EXCLUDED.next_offset,
    updated_at=NOW()
WHERE wallet_queue_offsets.next_offset < EXCLUDED.next_offset
"#;

const ENQUEUE_OUTBOX_MESSAGE_SQL: &str = r#"
INSERT INTO wallet_outbox(
    dedupe_key,
    topic,
    partition,
    message_key,
    payload_type,
    payload
)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(dedupe_key) DO NOTHING
"#;

const ENQUEUE_OUTBOX_MESSAGE_RETURNING_SQL: &str = r#"
INSERT INTO wallet_outbox(
    dedupe_key,
    topic,
    partition,
    message_key,
    payload_type,
    payload
)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(dedupe_key) DO NOTHING
RETURNING outbox_id
"#;

const OUTBOX_DUPLICATE_MATCHES_SQL: &str = r#"
SELECT EXISTS(
    SELECT 1
    FROM wallet_outbox
    WHERE dedupe_key=$1
      AND topic=$2
      AND partition IS NOT DISTINCT FROM $3::integer
      AND message_key=$4
      AND payload_type=$5
      AND payload=$6::jsonb
) AS matches
"#;

const CLAIM_OUTBOX_MESSAGES_SQL: &str = r#"
WITH claimed AS (
    SELECT outbox_id
    FROM wallet_outbox
    WHERE status='PENDING' AND next_attempt_at <= NOW()
    ORDER BY outbox_id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE wallet_outbox
SET status='PROCESSING',
    attempts=attempts+1,
    updated_at=NOW()
FROM claimed
WHERE wallet_outbox.outbox_id=claimed.outbox_id
RETURNING
    wallet_outbox.outbox_id,
    wallet_outbox.dedupe_key,
    wallet_outbox.topic,
    wallet_outbox.partition,
    wallet_outbox.message_key,
    wallet_outbox.payload_type,
    wallet_outbox.payload,
    wallet_outbox.attempts
"#;

const MARK_OUTBOX_PUBLISHED_SQL: &str = r#"
UPDATE wallet_outbox
SET status='PUBLISHED',
    published_at=NOW(),
    updated_at=NOW()
WHERE outbox_id=$1
"#;

const MARK_OUTBOX_PENDING_SQL: &str = r#"
UPDATE wallet_outbox
SET status='PENDING',
    last_error=$2,
    next_attempt_at=NOW() + INTERVAL '5 seconds',
    updated_at=NOW()
WHERE outbox_id=$1
"#;

const REQUEUE_STALE_OUTBOX_MESSAGES_SQL: &str = r#"
UPDATE wallet_outbox
SET status='PENDING',
    last_error='requeued stale PROCESSING message',
    next_attempt_at=NOW(),
    updated_at=NOW()
WHERE status='PROCESSING'
  AND updated_at <= NOW() - ($1::bigint * INTERVAL '1 second')
"#;

const LOAD_OUTBOX_METRICS_SQL: &str = r#"
SELECT
    COUNT(*) FILTER (WHERE status='PENDING')::BIGINT AS pending_count,
    COUNT(*) FILTER (WHERE status='PENDING' AND next_attempt_at <= NOW())::BIGINT AS ready_count,
    COUNT(*) FILTER (WHERE status='PROCESSING')::BIGINT AS processing_count,
    COUNT(*) FILTER (WHERE status='PUBLISHED')::BIGINT AS published_count,
    COUNT(*) FILTER (WHERE attempts > 0 AND status <> 'PUBLISHED')::BIGINT AS retry_count,
    COALESCE(MAX(attempts) FILTER (WHERE status <> 'PUBLISHED'), 0)::INTEGER
        AS max_unpublished_attempts,
    FLOOR(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status='PENDING'))))::BIGINT
        AS oldest_pending_age_seconds
FROM wallet_outbox
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceSnapshot {
    pub total: i64,
    pub locked: i64,
}

impl BalanceSnapshot {
    pub fn available(&self) -> i64 {
        self.total - self.locked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationRecord {
    pub reservation_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub amount: i64,
    pub remaining: i64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDeltaUpdate {
    pub user_id: i64,
    pub asset: Asset,
    pub total_delta: i64,
    pub locked_delta: i64,
    pub kind: String,
    pub reference_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewWalletOutboxMessage {
    pub dedupe_key: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub message_key: String,
    pub payload_type: String,
    pub payload: Value,
}

impl NewWalletOutboxMessage {
    pub fn json<T: Serialize>(
        dedupe_key: impl Into<String>,
        topic: impl Into<String>,
        partition: Option<i32>,
        message_key: impl Into<String>,
        payload_type: impl Into<String>,
        payload: &T,
    ) -> Result<Self, WalletRepositoryError> {
        Ok(Self {
            dedupe_key: dedupe_key.into(),
            topic: topic.into(),
            partition,
            message_key: message_key.into(),
            payload_type: payload_type.into(),
            payload: serde_json::to_value(payload)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WalletOutboxMessage {
    pub outbox_id: i64,
    pub dedupe_key: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub message_key: String,
    pub payload_type: String,
    pub payload: Value,
    pub attempts: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletOutboxMetrics {
    pub pending_count: i64,
    pub ready_count: i64,
    pub processing_count: i64,
    pub published_count: i64,
    pub retry_count: i64,
    pub max_unpublished_attempts: i32,
    pub oldest_pending_age_seconds: Option<i64>,
}

#[derive(Debug)]
pub enum WalletRepositoryError {
    InsufficientFunds { available: i64 },
    IdempotencyConflict,
    ReservationNotFound,
    InvalidReservationState,
    InvalidAccountDelta,
    Storage(sqlx::Error),
    Serialization(serde_json::Error),
}

impl From<sqlx::Error> for WalletRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl From<serde_json::Error> for WalletRepositoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct WalletRepository {
    pool: Pool<Postgres>,
}

impl WalletRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn get_idempotent_reply(
        &self,
        user_id: i64,
        command_type: &str,
        idempotency_key: &str,
    ) -> Result<Option<Value>, WalletRepositoryError> {
        let reply = sqlx::query(
            r#"
            SELECT reply_payload
            FROM wallet_idempotency
            WHERE user_id=$1 AND command_type=$2 AND idempotency_key=$3
            "#,
        )
        .bind(user_id)
        .bind(command_type)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.get::<Value, _>("reply_payload"));

        Ok(reply)
    }

    pub async fn record_idempotent_reply(
        &self,
        user_id: i64,
        command_type: &str,
        idempotency_key: &str,
        request_id: &str,
        reply_payload: Value,
    ) -> Result<(), WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;
        record_idempotent_reply_in_tx(
            &mut tx,
            user_id,
            command_type,
            idempotency_key,
            request_id,
            reply_payload,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn record_idempotent_reply_with_outbox(
        &self,
        user_id: i64,
        command_type: &str,
        idempotency_key: &str,
        request_id: &str,
        reply_payload: Value,
        outbox_messages: &[NewWalletOutboxMessage],
    ) -> Result<(), WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;

        record_idempotent_reply_in_tx(
            &mut tx,
            user_id,
            command_type,
            idempotency_key,
            request_id,
            reply_payload,
        )
        .await?;
        enqueue_outbox_messages_in_tx(&mut tx, outbox_messages).await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn reserve_funds(
        &self,
        user_id: i64,
        request_id: &str,
        idempotency_key: &str,
        asset: Asset,
        amount: i64,
    ) -> Result<FundsReserved, WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let db_asset = asset_to_db(asset);

        let balance = sqlx::query(
            r#"
            UPDATE user_collaterals
            SET locked=locked+$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND total-locked >= $3
            RETURNING total, locked
            "#,
        )
        .bind(user_id)
        .bind(db_asset)
        .bind(amount)
        .fetch_optional(&mut *tx)
        .await?;

        if balance.is_none() {
            let available = current_available_in_tx(&mut tx, user_id, db_asset).await?;
            tx.rollback().await?;
            return Err(WalletRepositoryError::InsufficientFunds { available });
        }

        let reservation_id = format!("res_{}", Uuid::new_v4());

        sqlx::query(
            r#"
            INSERT INTO wallet_reservations(
                reservation_id,
                user_id,
                asset,
                amount,
                remaining,
                status,
                idempotency_key,
                request_id
            )
            VALUES($1,$2,$3,$4,$4,'ACTIVE',$5,$6)
            "#,
        )
        .bind(&reservation_id)
        .bind(user_id)
        .bind(db_asset)
        .bind(amount)
        .bind(idempotency_key)
        .bind(request_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(FundsReserved {
            request_id: String::from(request_id),
            reservation_id,
            asset,
            amount,
        })
    }

    pub async fn reserve_funds_with_outbox<F>(
        &self,
        user_id: i64,
        request_id: &str,
        idempotency_key: &str,
        asset: Asset,
        amount: i64,
        command_type: &str,
        outbox_builder: F,
    ) -> Result<FundsReserved, WalletRepositoryError>
    where
        F: FnOnce(
            &FundsReserved,
        ) -> Result<(Value, Vec<NewWalletOutboxMessage>), WalletRepositoryError>,
    {
        let mut tx = self.pool.begin().await?;
        let db_asset = asset_to_db(asset);

        let balance = sqlx::query(
            r#"
            UPDATE user_collaterals
            SET locked=locked+$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND total-locked >= $3
            RETURNING total, locked
            "#,
        )
        .bind(user_id)
        .bind(db_asset)
        .bind(amount)
        .fetch_optional(&mut *tx)
        .await?;

        if balance.is_none() {
            let available = current_available_in_tx(&mut tx, user_id, db_asset).await?;
            tx.rollback().await?;
            return Err(WalletRepositoryError::InsufficientFunds { available });
        }

        let reservation_id = format!("res_{}", Uuid::new_v4());

        sqlx::query(
            r#"
            INSERT INTO wallet_reservations(
                reservation_id,
                user_id,
                asset,
                amount,
                remaining,
                status,
                idempotency_key,
                request_id
            )
            VALUES($1,$2,$3,$4,$4,'ACTIVE',$5,$6)
            "#,
        )
        .bind(&reservation_id)
        .bind(user_id)
        .bind(db_asset)
        .bind(amount)
        .bind(idempotency_key)
        .bind(request_id)
        .execute(&mut *tx)
        .await?;

        let reserved = FundsReserved {
            request_id: String::from(request_id),
            reservation_id,
            asset,
            amount,
        };
        let (reply_payload, outbox_messages) = outbox_builder(&reserved)?;

        record_idempotent_reply_in_tx(
            &mut tx,
            user_id,
            command_type,
            idempotency_key,
            request_id,
            reply_payload,
        )
        .await?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(reserved)
    }

    pub async fn apply_deposit(
        &self,
        deposit: &Deposit,
    ) -> Result<BalanceUpdated, WalletRepositoryError> {
        let db_asset = asset_to_db(deposit.asset);
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO user_collaterals(user_id, asset, total, locked)
            VALUES($1,$2,$3,0)
            ON CONFLICT(user_id, asset)
            DO UPDATE
            SET total=user_collaterals.total+EXCLUDED.total,
                updated_at=NOW()
            RETURNING total, locked
            "#,
        )
        .bind(deposit.envelope.user_id)
        .bind(db_asset)
        .bind(deposit.amount)
        .fetch_one(&mut *tx)
        .await?;

        insert_ledger_in_tx(
            &mut tx,
            deposit.envelope.user_id,
            db_asset,
            deposit.amount,
            "DEPOSIT",
            &deposit.reference_id,
        )
        .await?;

        tx.commit().await?;

        Ok(BalanceUpdated {
            request_id: deposit.envelope.request_id.clone(),
            asset: deposit.asset,
            total: row.get("total"),
            locked: row.get("locked"),
        })
    }

    pub async fn apply_deposit_with_outbox<F>(
        &self,
        deposit: &Deposit,
        command_type: &str,
        outbox_builder: F,
    ) -> Result<BalanceUpdated, WalletRepositoryError>
    where
        F: FnOnce(
            &BalanceUpdated,
        ) -> Result<(Value, Vec<NewWalletOutboxMessage>), WalletRepositoryError>,
    {
        let db_asset = asset_to_db(deposit.asset);
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO user_collaterals(user_id, asset, total, locked)
            VALUES($1,$2,$3,0)
            ON CONFLICT(user_id, asset)
            DO UPDATE
            SET total=user_collaterals.total+EXCLUDED.total,
                updated_at=NOW()
            RETURNING total, locked
            "#,
        )
        .bind(deposit.envelope.user_id)
        .bind(db_asset)
        .bind(deposit.amount)
        .fetch_one(&mut *tx)
        .await?;

        insert_ledger_in_tx(
            &mut tx,
            deposit.envelope.user_id,
            db_asset,
            deposit.amount,
            "DEPOSIT",
            &deposit.reference_id,
        )
        .await?;

        let balance = BalanceUpdated {
            request_id: deposit.envelope.request_id.clone(),
            asset: deposit.asset,
            total: row.get("total"),
            locked: row.get("locked"),
        };
        let (reply_payload, outbox_messages) = outbox_builder(&balance)?;

        record_idempotent_reply_in_tx(
            &mut tx,
            deposit.envelope.user_id,
            command_type,
            &deposit.envelope.idempotency_key,
            &deposit.envelope.request_id,
            reply_payload,
        )
        .await?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(balance)
    }

    pub async fn apply_withdraw(
        &self,
        withdraw: &Withdraw,
    ) -> Result<BalanceUpdated, WalletRepositoryError> {
        let db_asset = asset_to_db(withdraw.asset);
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            UPDATE user_collaterals
            SET total=total-$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND total-locked >= $3
            RETURNING total, locked
            "#,
        )
        .bind(withdraw.envelope.user_id)
        .bind(db_asset)
        .bind(withdraw.amount)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let available =
                current_available_in_tx(&mut tx, withdraw.envelope.user_id, db_asset).await?;
            tx.rollback().await?;
            return Err(WalletRepositoryError::InsufficientFunds { available });
        };

        insert_ledger_in_tx(
            &mut tx,
            withdraw.envelope.user_id,
            db_asset,
            -withdraw.amount,
            "WITHDRAW",
            &withdraw.envelope.idempotency_key,
        )
        .await?;

        tx.commit().await?;

        Ok(BalanceUpdated {
            request_id: withdraw.envelope.request_id.clone(),
            asset: withdraw.asset,
            total: row.get("total"),
            locked: row.get("locked"),
        })
    }

    pub async fn apply_withdraw_with_outbox<F>(
        &self,
        withdraw: &Withdraw,
        command_type: &str,
        outbox_builder: F,
    ) -> Result<BalanceUpdated, WalletRepositoryError>
    where
        F: FnOnce(
            &BalanceUpdated,
        ) -> Result<(Value, Vec<NewWalletOutboxMessage>), WalletRepositoryError>,
    {
        let db_asset = asset_to_db(withdraw.asset);
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            UPDATE user_collaterals
            SET total=total-$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND total-locked >= $3
            RETURNING total, locked
            "#,
        )
        .bind(withdraw.envelope.user_id)
        .bind(db_asset)
        .bind(withdraw.amount)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let available =
                current_available_in_tx(&mut tx, withdraw.envelope.user_id, db_asset).await?;
            tx.rollback().await?;
            return Err(WalletRepositoryError::InsufficientFunds { available });
        };

        insert_ledger_in_tx(
            &mut tx,
            withdraw.envelope.user_id,
            db_asset,
            -withdraw.amount,
            "WITHDRAW",
            &withdraw.envelope.idempotency_key,
        )
        .await?;

        let balance = BalanceUpdated {
            request_id: withdraw.envelope.request_id.clone(),
            asset: withdraw.asset,
            total: row.get("total"),
            locked: row.get("locked"),
        };
        let (reply_payload, outbox_messages) = outbox_builder(&balance)?;

        record_idempotent_reply_in_tx(
            &mut tx,
            withdraw.envelope.user_id,
            command_type,
            &withdraw.envelope.idempotency_key,
            &withdraw.envelope.request_id,
            reply_payload,
        )
        .await?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(balance)
    }

    pub async fn release_reservation(
        &self,
        release: &ReleaseReservation,
    ) -> Result<ReservationRecord, WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let reservation = reservation_in_tx(&mut tx, &release.reservation_id).await?;

        if reservation.status != "ACTIVE"
            || release.amount <= 0
            || release.amount > reservation.remaining
        {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidReservationState);
        }

        let db_asset = asset_to_db(reservation.asset);

        sqlx::query(
            r#"
            UPDATE user_collaterals
            SET locked=locked-$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND locked >= $3
            "#,
        )
        .bind(reservation.user_id)
        .bind(db_asset)
        .bind(release.amount)
        .execute(&mut *tx)
        .await?;

        let remaining = reservation.remaining - release.amount;
        let status = if remaining == 0 { "RELEASED" } else { "ACTIVE" };

        let row = sqlx::query(
            r#"
            UPDATE wallet_reservations
            SET remaining=$2,
                status=$3,
                updated_at=NOW()
            WHERE reservation_id=$1
            RETURNING reservation_id, user_id, asset, amount, remaining, status
            "#,
        )
        .bind(&release.reservation_id)
        .bind(remaining)
        .bind(status)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        reservation_from_row(row)
    }

    pub async fn release_reservation_with_outbox<F>(
        &self,
        release: &ReleaseReservation,
        outbox_builder: F,
    ) -> Result<ReservationRecord, WalletRepositoryError>
    where
        F: FnOnce(&ReservationRecord) -> Result<Vec<NewWalletOutboxMessage>, WalletRepositoryError>,
    {
        let mut tx = self.pool.begin().await?;
        let reservation = reservation_in_tx(&mut tx, &release.reservation_id).await?;

        if reservation.status != "ACTIVE"
            || release.amount <= 0
            || release.amount > reservation.remaining
        {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidReservationState);
        }

        let db_asset = asset_to_db(reservation.asset);

        sqlx::query(
            r#"
            UPDATE user_collaterals
            SET locked=locked-$3,
                updated_at=NOW()
            WHERE user_id=$1 AND asset=$2 AND locked >= $3
            "#,
        )
        .bind(reservation.user_id)
        .bind(db_asset)
        .bind(release.amount)
        .execute(&mut *tx)
        .await?;

        let remaining = reservation.remaining - release.amount;
        let status = if remaining == 0 { "RELEASED" } else { "ACTIVE" };

        let row = sqlx::query(
            r#"
            UPDATE wallet_reservations
            SET remaining=$2,
                status=$3,
                updated_at=NOW()
            WHERE reservation_id=$1
            RETURNING reservation_id, user_id, asset, amount, remaining, status
            "#,
        )
        .bind(&release.reservation_id)
        .bind(remaining)
        .bind(status)
        .fetch_one(&mut *tx)
        .await?;

        let updated = reservation_from_row(row)?;
        let outbox_messages = outbox_builder(&updated)?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(updated)
    }

    pub async fn settle_trade(
        &self,
        settle: &SettleTrade,
    ) -> Result<ReservationRecord, WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let reservation = reservation_in_tx(&mut tx, &settle.reservation_id).await?;

        if reservation.status != "ACTIVE"
            || reservation.asset != settle.debit_asset
            || settle.debit_amount <= 0
            || settle.debit_amount > reservation.remaining
        {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidReservationState);
        }

        let debit_asset = asset_to_db(settle.debit_asset);
        let credit_asset = asset_to_db(settle.credit_asset);

        match settlement_collateral_effect(settle) {
            SettlementCollateralEffect::UnlockReservedOnly { amount } => {
                sqlx::query(
                    r#"
                    UPDATE user_collaterals
                    SET locked=locked-$3,
                        updated_at=NOW()
                    WHERE user_id=$1 AND asset=$2 AND locked >= $3
                    "#,
                )
                .bind(reservation.user_id)
                .bind(debit_asset)
                .bind(amount)
                .execute(&mut *tx)
                .await?;
            }
            SettlementCollateralEffect::TransferDebitToCredit {
                debit_amount,
                credit_amount,
            } => {
                sqlx::query(
                    r#"
                    UPDATE user_collaterals
                    SET locked=locked-$3,
                        total=total-$3,
                        updated_at=NOW()
                    WHERE user_id=$1 AND asset=$2 AND locked >= $3 AND total >= $3
                    "#,
                )
                .bind(reservation.user_id)
                .bind(debit_asset)
                .bind(debit_amount)
                .execute(&mut *tx)
                .await?;

                sqlx::query(
                    r#"
                    INSERT INTO user_collaterals(user_id, asset, total, locked)
                    VALUES($1,$2,$3,0)
                    ON CONFLICT(user_id, asset)
                    DO UPDATE
                    SET total=user_collaterals.total+EXCLUDED.total,
                        updated_at=NOW()
                    "#,
                )
                .bind(reservation.user_id)
                .bind(credit_asset)
                .bind(credit_amount)
                .execute(&mut *tx)
                .await?;
            }
        }

        let reference_id = settle.fill_id.to_string();
        insert_ledger_in_tx(
            &mut tx,
            reservation.user_id,
            debit_asset,
            -settle.debit_amount,
            "TRADE_DEBIT",
            &reference_id,
        )
        .await?;
        insert_ledger_in_tx(
            &mut tx,
            reservation.user_id,
            credit_asset,
            settle.credit_amount,
            "TRADE_CREDIT",
            &reference_id,
        )
        .await?;

        let remaining = reservation.remaining - settle.debit_amount;
        let status = if remaining == 0 { "SETTLED" } else { "ACTIVE" };

        let row = sqlx::query(
            r#"
            UPDATE wallet_reservations
            SET remaining=$2,
                status=$3,
                updated_at=NOW()
            WHERE reservation_id=$1
            RETURNING reservation_id, user_id, asset, amount, remaining, status
            "#,
        )
        .bind(&settle.reservation_id)
        .bind(remaining)
        .bind(status)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        reservation_from_row(row)
    }

    pub async fn settle_trade_with_outbox<F>(
        &self,
        settle: &SettleTrade,
        outbox_builder: F,
    ) -> Result<ReservationRecord, WalletRepositoryError>
    where
        F: FnOnce(&ReservationRecord) -> Result<Vec<NewWalletOutboxMessage>, WalletRepositoryError>,
    {
        let mut tx = self.pool.begin().await?;
        let reservation = reservation_in_tx(&mut tx, &settle.reservation_id).await?;

        if reservation.status != "ACTIVE"
            || reservation.asset != settle.debit_asset
            || settle.debit_amount <= 0
            || settle.debit_amount > reservation.remaining
        {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidReservationState);
        }

        let debit_asset = asset_to_db(settle.debit_asset);
        let credit_asset = asset_to_db(settle.credit_asset);

        match settlement_collateral_effect(settle) {
            SettlementCollateralEffect::UnlockReservedOnly { amount } => {
                sqlx::query(
                    r#"
                    UPDATE user_collaterals
                    SET locked=locked-$3,
                        updated_at=NOW()
                    WHERE user_id=$1 AND asset=$2 AND locked >= $3
                    "#,
                )
                .bind(reservation.user_id)
                .bind(debit_asset)
                .bind(amount)
                .execute(&mut *tx)
                .await?;
            }
            SettlementCollateralEffect::TransferDebitToCredit {
                debit_amount,
                credit_amount,
            } => {
                sqlx::query(
                    r#"
                    UPDATE user_collaterals
                    SET locked=locked-$3,
                        total=total-$3,
                        updated_at=NOW()
                    WHERE user_id=$1 AND asset=$2 AND locked >= $3 AND total >= $3
                    "#,
                )
                .bind(reservation.user_id)
                .bind(debit_asset)
                .bind(debit_amount)
                .execute(&mut *tx)
                .await?;

                sqlx::query(
                    r#"
                    INSERT INTO user_collaterals(user_id, asset, total, locked)
                    VALUES($1,$2,$3,0)
                    ON CONFLICT(user_id, asset)
                    DO UPDATE
                    SET total=user_collaterals.total+EXCLUDED.total,
                        updated_at=NOW()
                    "#,
                )
                .bind(reservation.user_id)
                .bind(credit_asset)
                .bind(credit_amount)
                .execute(&mut *tx)
                .await?;
            }
        }

        let reference_id = settle.fill_id.to_string();
        insert_ledger_in_tx(
            &mut tx,
            reservation.user_id,
            debit_asset,
            -settle.debit_amount,
            "TRADE_DEBIT",
            &reference_id,
        )
        .await?;
        insert_ledger_in_tx(
            &mut tx,
            reservation.user_id,
            credit_asset,
            settle.credit_amount,
            "TRADE_CREDIT",
            &reference_id,
        )
        .await?;

        let remaining = reservation.remaining - settle.debit_amount;
        let status = if remaining == 0 { "SETTLED" } else { "ACTIVE" };

        let row = sqlx::query(
            r#"
            UPDATE wallet_reservations
            SET remaining=$2,
                status=$3,
                updated_at=NOW()
            WHERE reservation_id=$1
            RETURNING reservation_id, user_id, asset, amount, remaining, status
            "#,
        )
        .bind(&settle.reservation_id)
        .bind(remaining)
        .bind(status)
        .fetch_one(&mut *tx)
        .await?;

        let updated = reservation_from_row(row)?;
        let outbox_messages = outbox_builder(&updated)?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(updated)
    }

    pub async fn apply_account_delta(
        &self,
        delta: &AccountDeltaUpdate,
    ) -> Result<Option<BalanceSnapshot>, WalletRepositoryError> {
        if delta.total_delta == 0 && delta.locked_delta == 0 {
            return Ok(None);
        }

        let mut tx = self.pool.begin().await?;
        let db_asset = asset_to_db(delta.asset);

        let inserted = sqlx::query(
            r#"
            INSERT INTO wallet_ledger(user_id, asset, amount, kind, reference_id)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT(user_id, asset, kind, reference_id)
            DO NOTHING
            RETURNING ledger_id
            "#,
        )
        .bind(delta.user_id)
        .bind(db_asset)
        .bind(delta.total_delta)
        .bind(&delta.kind)
        .bind(&delta.reference_id)
        .fetch_optional(&mut *tx)
        .await?;

        if inserted.is_none() {
            tx.commit().await?;
            return Ok(None);
        }

        let balance = sqlx::query(
            r#"
            INSERT INTO user_collaterals(user_id, asset, total, locked)
            SELECT $1,$2,$3,$4
            WHERE $3 >= 0 AND $4 >= 0 AND $4 <= $3
            ON CONFLICT(user_id, asset)
            DO UPDATE
            SET total=user_collaterals.total+EXCLUDED.total,
                locked=user_collaterals.locked+EXCLUDED.locked,
                updated_at=NOW()
            WHERE user_collaterals.total+EXCLUDED.total >= 0
              AND user_collaterals.locked+EXCLUDED.locked >= 0
              AND user_collaterals.locked+EXCLUDED.locked
                    <= user_collaterals.total+EXCLUDED.total
            RETURNING total, locked
            "#,
        )
        .bind(delta.user_id)
        .bind(db_asset)
        .bind(delta.total_delta)
        .bind(delta.locked_delta)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(balance) = balance else {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidAccountDelta);
        };

        tx.commit().await?;

        Ok(Some(BalanceSnapshot {
            total: balance.get("total"),
            locked: balance.get("locked"),
        }))
    }

    pub async fn apply_account_delta_with_outbox<F>(
        &self,
        delta: &AccountDeltaUpdate,
        outbox_builder: F,
    ) -> Result<Option<BalanceSnapshot>, WalletRepositoryError>
    where
        F: FnOnce(&BalanceSnapshot) -> Result<Vec<NewWalletOutboxMessage>, WalletRepositoryError>,
    {
        if delta.total_delta == 0 && delta.locked_delta == 0 {
            return Ok(None);
        }

        let mut tx = self.pool.begin().await?;
        let db_asset = asset_to_db(delta.asset);

        let inserted = sqlx::query(
            r#"
            INSERT INTO wallet_ledger(user_id, asset, amount, kind, reference_id)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT(user_id, asset, kind, reference_id)
            DO NOTHING
            RETURNING ledger_id
            "#,
        )
        .bind(delta.user_id)
        .bind(db_asset)
        .bind(delta.total_delta)
        .bind(&delta.kind)
        .bind(&delta.reference_id)
        .fetch_optional(&mut *tx)
        .await?;

        if inserted.is_none() {
            tx.commit().await?;
            return Ok(None);
        }

        let balance = sqlx::query(
            r#"
            INSERT INTO user_collaterals(user_id, asset, total, locked)
            SELECT $1,$2,$3,$4
            WHERE $3 >= 0 AND $4 >= 0 AND $4 <= $3
            ON CONFLICT(user_id, asset)
            DO UPDATE
            SET total=user_collaterals.total+EXCLUDED.total,
                locked=user_collaterals.locked+EXCLUDED.locked,
                updated_at=NOW()
            WHERE user_collaterals.total+EXCLUDED.total >= 0
              AND user_collaterals.locked+EXCLUDED.locked >= 0
              AND user_collaterals.locked+EXCLUDED.locked
                    <= user_collaterals.total+EXCLUDED.total
            RETURNING total, locked
            "#,
        )
        .bind(delta.user_id)
        .bind(db_asset)
        .bind(delta.total_delta)
        .bind(delta.locked_delta)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(balance) = balance else {
            tx.rollback().await?;
            return Err(WalletRepositoryError::InvalidAccountDelta);
        };

        let snapshot = BalanceSnapshot {
            total: balance.get("total"),
            locked: balance.get("locked"),
        };
        let outbox_messages = outbox_builder(&snapshot)?;
        enqueue_outbox_messages_in_tx(&mut tx, &outbox_messages).await?;

        tx.commit().await?;

        Ok(Some(snapshot))
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, WalletRepositoryError> {
        let offset = sqlx::query(
            r#"
            SELECT next_offset
            FROM wallet_queue_offsets
            WHERE topic=$1 AND partition=$2
            "#,
        )
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.get("next_offset"));

        Ok(offset)
    }

    pub async fn enqueue_outbox_message(
        &self,
        message: &NewWalletOutboxMessage,
    ) -> Result<Option<i64>, WalletRepositoryError> {
        let row = sqlx::query(ENQUEUE_OUTBOX_MESSAGE_RETURNING_SQL)
            .bind(&message.dedupe_key)
            .bind(&message.topic)
            .bind(message.partition)
            .bind(&message.message_key)
            .bind(&message.payload_type)
            .bind(&message.payload)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            return Ok(Some(row.get("outbox_id")));
        }

        if self.outbox_duplicate_matches(message).await? {
            Ok(None)
        } else {
            Err(WalletRepositoryError::IdempotencyConflict)
        }
    }

    async fn outbox_duplicate_matches(
        &self,
        message: &NewWalletOutboxMessage,
    ) -> Result<bool, WalletRepositoryError> {
        let matches = sqlx::query_scalar::<_, bool>(OUTBOX_DUPLICATE_MATCHES_SQL)
            .bind(&message.dedupe_key)
            .bind(&message.topic)
            .bind(message.partition)
            .bind(&message.message_key)
            .bind(&message.payload_type)
            .bind(&message.payload)
            .fetch_one(&self.pool)
            .await?;

        Ok(matches)
    }

    pub async fn claim_outbox_messages(
        &self,
        limit: i64,
    ) -> Result<Vec<WalletOutboxMessage>, WalletRepositoryError> {
        let mut tx = self.pool.begin().await?;

        let rows = sqlx::query(CLAIM_OUTBOX_MESSAGES_SQL)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(rows.into_iter().map(outbox_message_from_row).collect())
    }

    pub async fn mark_outbox_published(&self, outbox_id: i64) -> Result<(), WalletRepositoryError> {
        sqlx::query(MARK_OUTBOX_PUBLISHED_SQL)
            .bind(outbox_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn mark_outbox_pending(
        &self,
        outbox_id: i64,
        error: &str,
    ) -> Result<(), WalletRepositoryError> {
        sqlx::query(MARK_OUTBOX_PENDING_SQL)
            .bind(outbox_id)
            .bind(error)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn requeue_stale_outbox_messages(
        &self,
        stale_after_seconds: i64,
    ) -> Result<u64, WalletRepositoryError> {
        let result = sqlx::query(REQUEUE_STALE_OUTBOX_MESSAGES_SQL)
            .bind(stale_after_seconds)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }

    pub async fn load_outbox_metrics(&self) -> Result<WalletOutboxMetrics, WalletRepositoryError> {
        let row = sqlx::query(LOAD_OUTBOX_METRICS_SQL)
            .fetch_one(&self.pool)
            .await?;

        Ok(WalletOutboxMetrics {
            pending_count: row.get("pending_count"),
            ready_count: row.get("ready_count"),
            processing_count: row.get("processing_count"),
            published_count: row.get("published_count"),
            retry_count: row.get("retry_count"),
            max_unpublished_attempts: row.get("max_unpublished_attempts"),
            oldest_pending_age_seconds: row.get("oldest_pending_age_seconds"),
        })
    }

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), WalletRepositoryError> {
        sqlx::query(SAVE_QUEUE_OFFSET_SQL)
            .bind(topic)
            .bind(partition)
            .bind(next_offset)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

async fn current_available_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    user_id: i64,
    asset: AssetType,
) -> Result<i64, WalletRepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT total, locked
        FROM user_collaterals
        WHERE user_id=$1 AND asset=$2
        "#,
    )
    .bind(user_id)
    .bind(asset)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(row) = row else {
        return Ok(0);
    };

    let snapshot = BalanceSnapshot {
        total: row.get("total"),
        locked: row.get("locked"),
    };

    Ok(snapshot.available())
}

async fn record_idempotent_reply_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    user_id: i64,
    command_type: &str,
    idempotency_key: &str,
    request_id: &str,
    reply_payload: Value,
) -> Result<(), WalletRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO wallet_idempotency(user_id, command_type, idempotency_key, request_id, reply_payload)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT(user_id, command_type, idempotency_key)
        DO UPDATE
        SET request_id=EXCLUDED.request_id,
            updated_at=NOW()
        "#,
    )
    .bind(user_id)
    .bind(command_type)
    .bind(idempotency_key)
    .bind(request_id)
    .bind(reply_payload)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn enqueue_outbox_messages_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    messages: &[NewWalletOutboxMessage],
) -> Result<(), WalletRepositoryError> {
    for message in messages {
        let result = sqlx::query(ENQUEUE_OUTBOX_MESSAGE_SQL)
            .bind(&message.dedupe_key)
            .bind(&message.topic)
            .bind(message.partition)
            .bind(&message.message_key)
            .bind(&message.payload_type)
            .bind(&message.payload)
            .execute(&mut **tx)
            .await?;

        if result.rows_affected() == 0 && !outbox_duplicate_matches_in_tx(tx, message).await? {
            return Err(WalletRepositoryError::IdempotencyConflict);
        }
    }

    Ok(())
}

async fn outbox_duplicate_matches_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    message: &NewWalletOutboxMessage,
) -> Result<bool, WalletRepositoryError> {
    let matches = sqlx::query_scalar::<_, bool>(OUTBOX_DUPLICATE_MATCHES_SQL)
        .bind(&message.dedupe_key)
        .bind(&message.topic)
        .bind(message.partition)
        .bind(&message.message_key)
        .bind(&message.payload_type)
        .bind(&message.payload)
        .fetch_one(&mut **tx)
        .await?;

    Ok(matches)
}

async fn insert_ledger_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    user_id: i64,
    asset: AssetType,
    amount: i64,
    kind: &str,
    reference_id: &str,
) -> Result<(), WalletRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO wallet_ledger(user_id, asset, amount, kind, reference_id)
        VALUES($1,$2,$3,$4,$5)
        "#,
    )
    .bind(user_id)
    .bind(asset)
    .bind(amount)
    .bind(kind)
    .bind(reference_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn reservation_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: &str,
) -> Result<ReservationRecord, WalletRepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT reservation_id, user_id, asset, amount, remaining, status
        FROM wallet_reservations
        WHERE reservation_id=$1
        "#,
    )
    .bind(reservation_id)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(row) = row else {
        return Err(WalletRepositoryError::ReservationNotFound);
    };

    reservation_from_row(row)
}

fn reservation_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ReservationRecord, WalletRepositoryError> {
    let db_asset: AssetType = row.get("asset");

    Ok(ReservationRecord {
        reservation_id: row.get("reservation_id"),
        user_id: row.get("user_id"),
        asset: asset_from_db(db_asset),
        amount: row.get("amount"),
        remaining: row.get("remaining"),
        status: row.get("status"),
    })
}

fn outbox_message_from_row(row: sqlx::postgres::PgRow) -> WalletOutboxMessage {
    WalletOutboxMessage {
        outbox_id: row.get("outbox_id"),
        dedupe_key: row.get("dedupe_key"),
        topic: row.get("topic"),
        partition: row.get("partition"),
        message_key: row.get("message_key"),
        payload_type: row.get("payload_type"),
        payload: row.get("payload"),
        attempts: row.get("attempts"),
    }
}

pub fn asset_to_db(asset: Asset) -> AssetType {
    match asset {
        Asset::USDC => AssetType::USDC,
        Asset::USDT => AssetType::USDT,
        Asset::SOL => AssetType::SOL,
        Asset::ETH => AssetType::ETH,
        Asset::BTC => AssetType::BTC,
        Asset::PERP => AssetType::PERP,
        Asset::HYP => AssetType::HYP,
    }
}

pub fn asset_from_db(asset: AssetType) -> Asset {
    match asset {
        AssetType::USDC => Asset::USDC,
        AssetType::USDT => Asset::USDT,
        AssetType::SOL => Asset::SOL,
        AssetType::ETH => Asset::ETH,
        AssetType::BTC => Asset::BTC,
        AssetType::PERP => Asset::PERP,
        AssetType::HYP => Asset::HYP,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementCollateralEffect {
    UnlockReservedOnly {
        amount: i64,
    },
    TransferDebitToCredit {
        debit_amount: i64,
        credit_amount: i64,
    },
}

fn settlement_collateral_effect(settle: &SettleTrade) -> SettlementCollateralEffect {
    if is_same_asset_reservation_consumption(settle) {
        SettlementCollateralEffect::UnlockReservedOnly {
            amount: settle.debit_amount,
        }
    } else {
        SettlementCollateralEffect::TransferDebitToCredit {
            debit_amount: settle.debit_amount,
            credit_amount: settle.credit_amount,
        }
    }
}

fn is_same_asset_reservation_consumption(settle: &SettleTrade) -> bool {
    settle.debit_asset == settle.credit_asset && settle.debit_amount == settle.credit_amount
}

pub fn insufficient_funds_reply(
    request_id: String,
    asset: Asset,
    required: i64,
    available: i64,
) -> InsufficientFunds {
    InsufficientFunds {
        request_id,
        asset,
        required,
        available,
    }
}

#[cfg(test)]
mod tests {
    use db::dto::AssetType;
    use protocol::{common::Asset, wallet::SettleTrade};
    use serde_json::json;

    use super::{
        CLAIM_OUTBOX_MESSAGES_SQL, LOAD_OUTBOX_METRICS_SQL, NewWalletOutboxMessage,
        OUTBOX_DUPLICATE_MATCHES_SQL, REQUEUE_STALE_OUTBOX_MESSAGES_SQL, SAVE_QUEUE_OFFSET_SQL,
        SettlementCollateralEffect, asset_from_db, asset_to_db,
        is_same_asset_reservation_consumption, settlement_collateral_effect,
    };

    #[test]
    fn asset_mapping_round_trips_all_known_assets() {
        let assets = [
            Asset::USDC,
            Asset::USDT,
            Asset::SOL,
            Asset::ETH,
            Asset::BTC,
            Asset::PERP,
            Asset::HYP,
        ];

        for asset in assets {
            assert_eq!(asset_from_db(asset_to_db(asset)), asset);
        }

        assert_eq!(asset_from_db(AssetType::BTC), Asset::BTC);
    }

    #[test]
    fn exact_same_asset_settlement_unlocks_reserved_collateral() {
        let settle = SettleTrade {
            fill_id: 7,
            reservation_id: String::from("res-1"),
            debit_asset: Asset::USDC,
            debit_amount: 100,
            credit_asset: Asset::USDC,
            credit_amount: 100,
        };

        assert!(is_same_asset_reservation_consumption(&settle));
        assert_eq!(
            settlement_collateral_effect(&settle),
            SettlementCollateralEffect::UnlockReservedOnly { amount: 100 }
        );
    }

    #[test]
    fn cross_asset_settlement_keeps_spot_like_collateral_path() {
        let settle = SettleTrade {
            fill_id: 7,
            reservation_id: String::from("res-1"),
            debit_asset: Asset::USDC,
            debit_amount: 100,
            credit_asset: Asset::SOL,
            credit_amount: 10,
        };

        assert!(!is_same_asset_reservation_consumption(&settle));
        assert_eq!(
            settlement_collateral_effect(&settle),
            SettlementCollateralEffect::TransferDebitToCredit {
                debit_amount: 100,
                credit_amount: 10,
            }
        );
    }

    #[test]
    fn same_asset_unequal_amount_settlement_keeps_spot_like_collateral_path() {
        let settle = SettleTrade {
            fill_id: 7,
            reservation_id: String::from("res-1"),
            debit_asset: Asset::USDC,
            debit_amount: 100,
            credit_asset: Asset::USDC,
            credit_amount: 99,
        };

        assert!(!is_same_asset_reservation_consumption(&settle));
        assert_eq!(
            settlement_collateral_effect(&settle),
            SettlementCollateralEffect::TransferDebitToCredit {
                debit_amount: 100,
                credit_amount: 99,
            }
        );
    }

    #[test]
    fn wallet_outbox_message_serializes_json_payload() {
        let message = NewWalletOutboxMessage::json(
            "engine-command:res-1",
            "engine.input",
            None,
            "input-1",
            "EngineCommand",
            &json!({
                "type": "PlaceOrder",
                "payload": {
                    "reservation_id": "res-1"
                }
            }),
        )
        .expect("outbox payload should serialize");

        assert_eq!(message.dedupe_key, "engine-command:res-1");
        assert_eq!(message.topic, "engine.input");
        assert_eq!(message.partition, None);
        assert_eq!(message.message_key, "input-1");
        assert_eq!(message.payload_type, "EngineCommand");
        assert_eq!(message.payload["payload"]["reservation_id"], "res-1");
    }

    #[test]
    fn outbox_claim_uses_skip_locked_and_attempt_tracking() {
        let sql = CLAIM_OUTBOX_MESSAGES_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("WHERE status='PENDING' AND next_attempt_at <= NOW()"));
        assert!(sql.contains("SET status='PROCESSING', attempts=attempts+1"));
        assert!(sql.contains("ORDER BY outbox_id"));
    }

    #[test]
    fn stale_processing_requeue_only_targets_stale_processing_rows() {
        let sql = REQUEUE_STALE_OUTBOX_MESSAGES_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("WHERE status='PROCESSING'"));
        assert!(sql.contains("updated_at <= NOW() - ($1::bigint * INTERVAL '1 second')"));
        assert!(sql.contains("last_error='requeued stale PROCESSING message'"));
        assert!(sql.contains("next_attempt_at=NOW()"));
    }

    #[test]
    fn outbox_metrics_reports_backlog_retry_and_age() {
        let sql = LOAD_OUTBOX_METRICS_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("COUNT(*) FILTER (WHERE status='PENDING')::BIGINT"));
        assert!(sql.contains(
            "COUNT(*) FILTER (WHERE status='PENDING' AND next_attempt_at <= NOW())::BIGINT"
        ));
        assert!(sql.contains("COUNT(*) FILTER (WHERE status='PROCESSING')::BIGINT"));
        assert!(sql.contains("COUNT(*) FILTER (WHERE status='PUBLISHED')::BIGINT"));
        assert!(
            sql.contains("COUNT(*) FILTER (WHERE attempts > 0 AND status <> 'PUBLISHED')::BIGINT")
        );
        assert!(
            sql.contains(
                "COALESCE(MAX(attempts) FILTER (WHERE status <> 'PUBLISHED'), 0)::INTEGER"
            )
        );
        assert!(sql.contains("oldest_pending_age_seconds"));
    }

    #[test]
    fn outbox_duplicate_check_compares_full_message_identity() {
        let sql = OUTBOX_DUPLICATE_MATCHES_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("WHERE dedupe_key=$1"));
        assert!(sql.contains("AND topic=$2"));
        assert!(sql.contains("AND partition IS NOT DISTINCT FROM $3::integer"));
        assert!(sql.contains("AND message_key=$4"));
        assert!(sql.contains("AND payload_type=$5"));
        assert!(sql.contains("AND payload=$6::jsonb"));
    }

    #[test]
    fn queue_offset_upsert_only_advances_offset() {
        let sql = SAVE_QUEUE_OFFSET_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("ON CONFLICT(topic, partition) DO UPDATE"));
        assert!(sql.contains("WHERE wallet_queue_offsets.next_offset < EXCLUDED.next_offset"));
    }
}
