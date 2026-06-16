use crate::{
    dto::{AssetType, UserCollateralRow},
    pool::pool,
};

pub async fn create_user_collaterals(
    user_id: i64,
    asset: AssetType,
    total: i64,
    locked: i64,
) -> Result<UserCollateralRow, sqlx::Error> {
    let created_user_collateral = sqlx::query_as!(
        UserCollateralRow,
        r#"
    INSERT INTO user_collaterals(user_id, asset, total, locked)
    VALUES($1,$2,$3,$4)
    RETURNING 
        user_id,
        asset as "asset!: AssetType",
        total,
        locked,
        created_at,
        updated_at
    "#,
        user_id,
        asset as AssetType,
        total,
        locked
    )
    .fetch_one(pool())
    .await?;

    Ok(created_user_collateral)
}

pub async fn add_total_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<i64, sqlx::Error> {
    let updated_total = sqlx::query_scalar!(
        r#"
    UPDATE user_collaterals
    SET total=total+$3,
        updated_at=NOW()
    WHERE user_id=$1 AND asset=$2
    RETURNING total
    "#,
        user_id,
        asset as AssetType,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(updated_total)
}

pub async fn update_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<i64, sqlx::Error> {
    add_total_collateral(asset as AssetType, amount, user_id).await
}

pub async fn udpate_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<i64, sqlx::Error> {
    add_total_collateral(asset as AssetType, amount, user_id).await
}

pub async fn add_locked_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<i64, sqlx::Error> {
    let updated_locked = sqlx::query_scalar!(
        r#"
    UPDATE user_collaterals
    SET locked=locked+$3,
        updated_at=NOW()
    WHERE user_id=$1 AND asset=$2
    RETURNING locked
    "#,
        user_id,
        asset as AssetType,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(updated_locked)
}

pub async fn update_locked(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<i64, sqlx::Error> {
    add_locked_collateral(asset as AssetType, amount, user_id).await
}

pub async fn lock_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<UserCollateralRow, sqlx::Error> {
    let collateral = sqlx::query_as!(
        UserCollateralRow,
        r#"
        UPDATE user_collaterals
        SET locked=locked+$3,
            updated_at=NOW()
        WHERE user_id=$1 AND asset=$2 AND locked+$3<=total
        RETURNING 
            user_id,
            asset as "asset!: AssetType",
            total,
            locked,
            created_at,
            updated_at
        "#,
        user_id,
        asset as AssetType,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(collateral)
}

pub async fn unlock_collateral(
    asset: AssetType,
    amount: i64,
    user_id: i64,
) -> Result<UserCollateralRow, sqlx::Error> {
    let collateral = sqlx::query_as!(
        UserCollateralRow,
        r#"
        UPDATE user_collaterals
        SET locked=locked-$3,
            updated_at=NOW()
        WHERE user_id=$1 AND asset=$2 AND locked>=$3
        RETURNING 
            user_id,
            asset as "asset!: AssetType",
            total,
            locked,
            created_at,
            updated_at
    "#,
        user_id,
        asset as AssetType,
        amount,
    )
    .fetch_one(pool())
    .await?;

    Ok(collateral)
}

pub async fn get_collaterals(user_id: i64) -> Result<Vec<UserCollateralRow>, sqlx::Error> {
    let collaterals = sqlx::query_as!(
        UserCollateralRow,
        r#"
        SELECT
            user_id,
            asset as "asset!: AssetType",
            total,
            locked,
            created_at,
            updated_at
        FROM user_collaterals WHERE user_id=$1
        "#,
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(collaterals)
}

pub async fn get_collateral_by_asset(
    user_id: i64,
    asset: AssetType,
) -> Result<UserCollateralRow, sqlx::Error> {
    let collateral = sqlx::query_as!(
        UserCollateralRow,
        r#"
        SELECT
            user_id,
            asset as "asset!: AssetType",
            total,
            locked,
            created_at,
            updated_at
        FROM user_collaterals
        WHERE user_id=$1 AND asset=$2
        "#,
        user_id,
        asset as AssetType
    )
    .fetch_one(pool())
    .await?;

    Ok(collateral)
}
