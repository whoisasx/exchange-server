use db::dto::AssetType;
use protocol::{
    common::Asset,
    wallet::{
        BalanceUpdated, Deposit, FundsReserved, InsufficientFunds, ReleaseReservation, SettleTrade,
        Withdraw,
    },
};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

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

#[derive(Debug)]
pub enum WalletRepositoryError {
    InsufficientFunds { available: i64 },
    IdempotencyConflict,
    ReservationNotFound,
    InvalidReservationState,
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
        .execute(&self.pool)
        .await?;

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
        .bind(settle.debit_amount)
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
        .bind(settle.credit_amount)
        .execute(&mut *tx)
        .await?;

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

    pub async fn save_queue_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), WalletRepositoryError> {
        sqlx::query(
            r#"
            INSERT INTO wallet_queue_offsets(topic, partition, next_offset)
            VALUES($1,$2,$3)
            ON CONFLICT(topic, partition)
            DO UPDATE
            SET next_offset=EXCLUDED.next_offset,
                updated_at=NOW()
            "#,
        )
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
    use protocol::common::Asset;

    use super::{asset_from_db, asset_to_db};

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
}
