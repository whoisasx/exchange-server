use chrono::{DateTime, Utc};

use crate::{
    dto::{FillRow, SideType},
    pool::pool,
};

pub async fn create_fill(
    fill_id: &str,
    maker_id: &str,
    taker_id: &str,
    maker_order_id: &str,
    taker_order_id: &str,
    price: i64,
    quantity: i64,
    maker_position: SideType,
    taker_position: SideType,
    created_at: DateTime<Utc>,
) -> Result<FillRow, sqlx::Error> {
    let fill = sqlx::query_as!(
        FillRow,
        r#"
        INSERT INTO fills(fill_id, maker_id, taker_id, maker_order_id, taker_order_id, price, quantity, maker_position, taker_position, created_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        RETURNING
            fill_id,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position as "maker_position!: SideType",
            taker_position as "taker_position!: SideType",
            created_at
        "#,
        fill_id,
        maker_id,
        taker_id,
        maker_order_id,
        taker_order_id,
        price,
        quantity,
        maker_position as SideType,
        taker_position as SideType,
        created_at
    )
    .fetch_one(pool())
    .await?;

    Ok(fill)
}

pub async fn get_fill_by_id(fill_id: &str) -> Result<Option<FillRow>, sqlx::Error> {
    let fill = sqlx::query_as!(
        FillRow,
        r#"
        SELECT
            fill_id,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position as "maker_position!: SideType",
            taker_position as "taker_position!: SideType",
            created_at
        FROM fills
        WHERE fill_id=$1
        "#,
        fill_id
    )
    .fetch_optional(pool())
    .await?;

    Ok(fill)
}

pub async fn get_fills_by_order_id(order_id: &str) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as!(
        FillRow,
        r#"
        SELECT
            fill_id,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position as "maker_position!: SideType",
            taker_position as "taker_position!: SideType",
            created_at
        FROM fills
        WHERE maker_order_id=$1 OR taker_order_id=$1
        ORDER BY created_at DESC
        "#,
        order_id
    )
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_user_id(user_id: &str) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as!(
        FillRow,
        r#"
        SELECT
            fill_id,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position as "maker_position!: SideType",
            taker_position as "taker_position!: SideType",
            created_at
        FROM fills
        WHERE maker_id=$1 OR taker_id=$1
        ORDER BY created_at DESC
        "#,
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_position_id(position_id: &str) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as!(
        FillRow,
        r#"
        SELECT
            fills.fill_id,
            fills.maker_id,
            fills.taker_id,
            fills.maker_order_id,
            fills.taker_order_id,
            fills.price,
            fills.quantity,
            fills.maker_position as "maker_position!: SideType",
            fills.taker_position as "taker_position!: SideType",
            fills.created_at
        FROM fills
        INNER JOIN position_fills ON position_fills.fill_id=fills.fill_id
        WHERE position_fills.position_id=$1
        ORDER BY fills.created_at DESC
        "#,
        position_id
    )
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_closed_position_id(
    position_id: &str,
) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as!(
        FillRow,
        r#"
        SELECT
            fills.fill_id,
            fills.maker_id,
            fills.taker_id,
            fills.maker_order_id,
            fills.taker_order_id,
            fills.price,
            fills.quantity,
            fills.maker_position as "maker_position!: SideType",
            fills.taker_position as "taker_position!: SideType",
            fills.created_at
        FROM fills
        INNER JOIN closed_position_fills ON closed_position_fills.fill_id=fills.fill_id
        WHERE closed_position_fills.position_id=$1
        ORDER BY fills.created_at DESC
        "#,
        position_id
    )
    .fetch_all(pool())
    .await?;

    Ok(fills)
}
