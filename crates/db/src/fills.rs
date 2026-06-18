use chrono::{DateTime, Utc};

use crate::{
    dto::{FillRow, SideType},
    pool::pool,
};

pub async fn create_fill(
    fill_id: i64,
    market_id: i64,
    engine_sequence: i64,
    maker_id: i64,
    taker_id: i64,
    maker_order_id: i64,
    taker_order_id: i64,
    price: i64,
    quantity: i64,
    maker_position: SideType,
    taker_position: SideType,
    executed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> Result<FillRow, sqlx::Error> {
    let fill = sqlx::query_as::<_, FillRow>(
        r#"
        INSERT INTO fills(
            fill_id,
            market_id,
            engine_sequence,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position,
            taker_position,
            executed_at,
            created_at
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        RETURNING
            fill_id,
            market_id,
            engine_sequence,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position,
            taker_position,
            executed_at,
            created_at
        "#,
    )
    .bind(fill_id)
    .bind(market_id)
    .bind(engine_sequence)
    .bind(maker_id)
    .bind(taker_id)
    .bind(maker_order_id)
    .bind(taker_order_id)
    .bind(price)
    .bind(quantity)
    .bind(maker_position)
    .bind(taker_position)
    .bind(executed_at)
    .bind(created_at)
    .fetch_one(pool())
    .await?;

    Ok(fill)
}

pub async fn get_fill_by_id(fill_id: i64) -> Result<Option<FillRow>, sqlx::Error> {
    let fill = sqlx::query_as::<_, FillRow>(
        r#"
        SELECT
            fill_id,
            market_id,
            engine_sequence,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position,
            taker_position,
            executed_at,
            created_at
        FROM fills
        WHERE fill_id=$1
        "#,
    )
    .bind(fill_id)
    .fetch_optional(pool())
    .await?;

    Ok(fill)
}

pub async fn get_fills_by_order_id(order_id: i64) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as::<_, FillRow>(
        r#"
        SELECT
            fill_id,
            market_id,
            engine_sequence,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position,
            taker_position,
            executed_at,
            created_at
        FROM fills
        WHERE maker_order_id=$1 OR taker_order_id=$1
        ORDER BY executed_at DESC, fill_id DESC
        "#,
    )
    .bind(order_id)
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_user_id(user_id: i64) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as::<_, FillRow>(
        r#"
        SELECT
            fill_id,
            market_id,
            engine_sequence,
            maker_id,
            taker_id,
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_position,
            taker_position,
            executed_at,
            created_at
        FROM fills
        WHERE maker_id=$1 OR taker_id=$1
        ORDER BY executed_at DESC, fill_id DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_position_id(position_id: i64) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as::<_, FillRow>(
        r#"
        SELECT
            fills.fill_id,
            fills.market_id,
            fills.engine_sequence,
            fills.maker_id,
            fills.taker_id,
            fills.maker_order_id,
            fills.taker_order_id,
            fills.price,
            fills.quantity,
            fills.maker_position,
            fills.taker_position,
            fills.executed_at,
            fills.created_at
        FROM fills
        INNER JOIN position_fills ON position_fills.fill_id=fills.fill_id
        WHERE position_fills.position_id=$1
        ORDER BY fills.executed_at DESC, fills.fill_id DESC
        "#,
    )
    .bind(position_id)
    .fetch_all(pool())
    .await?;

    Ok(fills)
}

pub async fn get_fills_by_closed_position_id(
    position_id: i64,
) -> Result<Vec<FillRow>, sqlx::Error> {
    let fills = sqlx::query_as::<_, FillRow>(
        r#"
        SELECT
            fills.fill_id,
            fills.market_id,
            fills.engine_sequence,
            fills.maker_id,
            fills.taker_id,
            fills.maker_order_id,
            fills.taker_order_id,
            fills.price,
            fills.quantity,
            fills.maker_position,
            fills.taker_position,
            fills.executed_at,
            fills.created_at
        FROM fills
        INNER JOIN closed_position_fills ON closed_position_fills.fill_id=fills.fill_id
        WHERE closed_position_fills.position_id=$1
        ORDER BY fills.executed_at DESC, fills.fill_id DESC
        "#,
    )
    .bind(position_id)
    .fetch_all(pool())
    .await?;

    Ok(fills)
}
