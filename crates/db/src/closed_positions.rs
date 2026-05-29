use chrono::{DateTime, Utc};

use crate::{
    dto::{CloseType, ClosedPositionRow, SideType},
    pool::pool,
};

pub async fn create_closed_position(
    position_id: &str,
    user_id: &str,
    market_id: &str,
    market_name: &str,
    side: SideType,
    quantity: i64,
    entry_price: i64,
    exit_price: i64,
    realized_pnl: i64,
    initial_margin: i64,
    closing_fee: i64,
    opened_at: DateTime<Utc>,
    closed_at: DateTime<Utc>,
    open_order_id: &str,
    close_order_id: &str,
    close_reason: CloseType,
) -> Result<ClosedPositionRow, sqlx::Error> {
    let closed_position = sqlx::query_as!(
        ClosedPositionRow,
        r#"
        INSERT INTO closed_positions(position_id, user_id, market_id, market_name, side, quantity, entry_price, exit_price, realized_pnl, initial_margin, closing_fee, opened_at, closed_at, open_order_id, close_order_id, close_reason)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        RETURNING
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            entry_price,
            exit_price,
            realized_pnl,
            initial_margin,
            closing_fee,
            opened_at,
            closed_at,
            open_order_id,
            close_order_id,
            close_reason as "close_reason!: CloseType"
        "#,
        position_id,
        user_id,
        market_id,
        market_name,
        side as SideType,
        quantity,
        entry_price,
        exit_price,
        realized_pnl,
        initial_margin,
        closing_fee,
        opened_at,
        closed_at,
        open_order_id,
        close_order_id,
        close_reason as CloseType
    )
    .fetch_one(pool())
    .await?;

    Ok(closed_position)
}

pub async fn get_closed_position_by_id(
    position_id: &str,
) -> Result<Option<ClosedPositionRow>, sqlx::Error> {
    let closed_position = sqlx::query_as!(
        ClosedPositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            entry_price,
            exit_price,
            realized_pnl,
            initial_margin,
            closing_fee,
            opened_at,
            closed_at,
            open_order_id,
            close_order_id,
            close_reason as "close_reason!: CloseType"
        FROM closed_positions
        WHERE position_id=$1
        "#,
        position_id
    )
    .fetch_optional(pool())
    .await?;

    Ok(closed_position)
}

pub async fn get_closed_positions_by_user_id(
    user_id: &str,
) -> Result<Vec<ClosedPositionRow>, sqlx::Error> {
    let closed_positions = sqlx::query_as!(
        ClosedPositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            entry_price,
            exit_price,
            realized_pnl,
            initial_margin,
            closing_fee,
            opened_at,
            closed_at,
            open_order_id,
            close_order_id,
            close_reason as "close_reason!: CloseType"
        FROM closed_positions
        WHERE user_id=$1
        ORDER BY closed_at DESC
        "#,
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(closed_positions)
}

pub async fn get_closed_positions_by_market_id(
    market_id: &str,
) -> Result<Vec<ClosedPositionRow>, sqlx::Error> {
    let closed_positions = sqlx::query_as!(
        ClosedPositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            entry_price,
            exit_price,
            realized_pnl,
            initial_margin,
            closing_fee,
            opened_at,
            closed_at,
            open_order_id,
            close_order_id,
            close_reason as "close_reason!: CloseType"
        FROM closed_positions
        WHERE market_id=$1
        ORDER BY closed_at DESC
        "#,
        market_id
    )
    .fetch_all(pool())
    .await?;

    Ok(closed_positions)
}

pub async fn get_closed_positions_by_user_market(
    user_id: &str,
    market_id: &str,
) -> Result<Vec<ClosedPositionRow>, sqlx::Error> {
    let closed_positions = sqlx::query_as!(
        ClosedPositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            entry_price,
            exit_price,
            realized_pnl,
            initial_margin,
            closing_fee,
            opened_at,
            closed_at,
            open_order_id,
            close_order_id,
            close_reason as "close_reason!: CloseType"
        FROM closed_positions
        WHERE user_id=$1 AND market_id=$2
        ORDER BY closed_at DESC
        "#,
        user_id,
        market_id
    )
    .fetch_all(pool())
    .await?;

    Ok(closed_positions)
}
