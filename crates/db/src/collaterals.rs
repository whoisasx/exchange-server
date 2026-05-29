use chrono::{DateTime, Utc};

use crate::{dto::UserCollateralRow, pool::pool};

pub async fn create_user_collaterals(
    user_id: &str,
    asset: &str,
    total: i64,
    locked: i64,
    created_at: DateTime<Utc>,
) -> Result<UserCollateralRow, sqlx::Error> {
    let created_user_collateral = sqlx::query_as!(
        UserCollateralRow,
        r#"
    INSERT INTO user_collaterals(user_id, asset, total, locked, updated_at, created_at)
    VALUES($1,$2,$3,$4,$5,$6)
    RETURNING *
    "#,
        user_id,
        asset,
        total,
        locked,
        created_at,
        created_at
    )
    .fetch_one(pool())
    .await?;

    Ok(created_user_collateral)
}

pub async fn udpate_collateral(
    asset: &str,
    amount: i64,
    user_id: &str,
) -> Result<i64, sqlx::Error> {
    let updated_total = sqlx::query_scalar!(
        r#"
    UPDATE user_collaterals
    SET total=total+$3
    WHERE user_id=$1 AND asset=$2
    RETURNING total
    "#,
        user_id,
        asset,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(updated_total)
}

pub async fn update_locked(asset: &str, amount: i64, user_id: &str) -> Result<i64, sqlx::Error> {
    let updated_locked = sqlx::query_scalar!(
        r#"
    UPDATE user_collaterals
    SET locked=locked+$3
    WHERE user_id=$1 AND asset=$2
    RETURNING locked
    "#,
        user_id,
        asset,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(updated_locked)
}

pub async fn get_collaterals(user_id: &str) -> Result<Vec<UserCollateralRow>, sqlx::Error> {
    let collaterals = sqlx::query_as!(
        UserCollateralRow,
        "SELECT * FROM user_collaterals WHERE user_id=$1",
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(collaterals)
}

pub async fn get_collateral_by_asset(
    user_id: &str,
    asset: &str,
) -> Result<UserCollateralRow, sqlx::Error> {
    let collateral = sqlx::query_as!(
        UserCollateralRow,
        "SELECT * FROM user_collaterals WHERE user_id=$1 AND asset=$2",
        user_id,
        asset
    )
    .fetch_one(pool())
    .await?;

    Ok(collateral)
}
