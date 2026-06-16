use chrono::{DateTime, Utc};

use crate::{
    dto::{CloseType, ClosedPositionRow, MarginType, PositionRow, SideType},
    pool::pool,
};

pub async fn create_position(
    position_id: i64,
    user_id: i64,
    market_id: i64,
    market_name: &str,
    side: SideType,
    quantity: i64,
    unrealized_pnl: i64,
    maintenance_margin: i64,
    initial_margin: i64,
    margin_chosen: MarginType,
    liquidation_price: i64,
    average_price: i64,
    opened_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> Result<PositionRow, sqlx::Error> {
    let position = sqlx::query_as!(
        PositionRow,
        r#"
        INSERT INTO positions(position_id, user_id, market_id, market_name, side, quantity, unrealized_pnl, maintenance_margin, initial_margin, margin_chosen, liquidation_price, average_price, opened_at, updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        RETURNING
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            unrealized_pnl,
            maintenance_margin,
            initial_margin,
            margin_chosen as "margin_chosen!: MarginType",
            liquidation_price,
            average_price,
            opened_at,
            updated_at
        "#,
        position_id,
        user_id,
        market_id,
        market_name,
        side as SideType,
        quantity,
        unrealized_pnl,
        maintenance_margin,
        initial_margin,
        margin_chosen as MarginType,
        liquidation_price,
        average_price,
        opened_at,
        updated_at
    )
    .fetch_one(pool())
    .await?;

    Ok(position)
}

pub async fn get_position_by_id(position_id: i64) -> Result<Option<PositionRow>, sqlx::Error> {
    let position = sqlx::query_as!(
        PositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            unrealized_pnl,
            maintenance_margin,
            initial_margin,
            margin_chosen as "margin_chosen!: MarginType",
            liquidation_price,
            average_price,
            opened_at,
            updated_at
        FROM positions
        WHERE position_id=$1
        "#,
        position_id
    )
    .fetch_optional(pool())
    .await?;

    Ok(position)
}

pub async fn get_position_by_user_market(
    user_id: i64,
    market_id: i64,
) -> Result<Option<PositionRow>, sqlx::Error> {
    let position = sqlx::query_as!(
        PositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            unrealized_pnl,
            maintenance_margin,
            initial_margin,
            margin_chosen as "margin_chosen!: MarginType",
            liquidation_price,
            average_price,
            opened_at,
            updated_at
        FROM positions
        WHERE user_id=$1 AND market_id=$2
        "#,
        user_id,
        market_id
    )
    .fetch_optional(pool())
    .await?;

    Ok(position)
}

pub async fn get_positions_by_user_id(user_id: i64) -> Result<Vec<PositionRow>, sqlx::Error> {
    let positions = sqlx::query_as!(
        PositionRow,
        r#"
        SELECT
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            unrealized_pnl,
            maintenance_margin,
            initial_margin,
            margin_chosen as "margin_chosen!: MarginType",
            liquidation_price,
            average_price,
            opened_at,
            updated_at
        FROM positions
        WHERE user_id=$1
        ORDER BY opened_at DESC
        "#,
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(positions)
}

pub async fn update_position(
    position_id: i64,
    quantity: i64,
    unrealized_pnl: i64,
    maintenance_margin: i64,
    initial_margin: i64,
    margin_chosen: MarginType,
    liquidation_price: i64,
    average_price: i64,
    updated_at: DateTime<Utc>,
) -> Result<PositionRow, sqlx::Error> {
    let position = sqlx::query_as!(
        PositionRow,
        r#"
        UPDATE positions
        SET quantity=$2,
            unrealized_pnl=$3,
            maintenance_margin=$4,
            initial_margin=$5,
            margin_chosen=$6,
            liquidation_price=$7,
            average_price=$8,
            updated_at=$9
        WHERE position_id=$1
        RETURNING
            position_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            quantity,
            unrealized_pnl,
            maintenance_margin,
            initial_margin,
            margin_chosen as "margin_chosen!: MarginType",
            liquidation_price,
            average_price,
            opened_at,
            updated_at
        "#,
        position_id,
        quantity,
        unrealized_pnl,
        maintenance_margin,
        initial_margin,
        margin_chosen as MarginType,
        liquidation_price,
        average_price,
        updated_at
    )
    .fetch_one(pool())
    .await?;

    Ok(position)
}

pub async fn delete_position(position_id: i64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"
        DELETE FROM positions
        WHERE position_id=$1
        "#,
        position_id
    )
    .execute(pool())
    .await?;

    Ok(result.rows_affected())
}

pub async fn close_position(
    position_id: i64,
    user_id: i64,
    market_id: i64,
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
    open_order_id: i64,
    close_order_id: i64,
    close_reason: CloseType,
    fill_ids: &[i64],
) -> Result<ClosedPositionRow, sqlx::Error> {
    let mut transaction = pool().begin().await?;

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
    .fetch_one(&mut *transaction)
    .await?;

    for fill_id in fill_ids {
        sqlx::query!(
            r#"
            INSERT INTO closed_position_fills(position_id, fill_id)
            VALUES($1,$2)
            "#,
            position_id,
            fill_id
        )
        .execute(&mut *transaction)
        .await?;
    }

    sqlx::query!(
        r#"
        DELETE FROM positions
        WHERE position_id=$1
        "#,
        position_id
    )
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;

    Ok(closed_position)
}
