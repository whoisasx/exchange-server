use crate::{
    dto::{OrderBookLevelRow, OrderBookSnapshot},
    pool::pool,
};

pub const DEFAULT_ORDERBOOK_DEPTH: i64 = 50;
pub const MAX_ORDERBOOK_DEPTH: i64 = 200;

pub async fn get_orderbook_snapshot(
    market_id: i64,
    depth: i64,
) -> Result<OrderBookSnapshot, sqlx::Error> {
    let state = sqlx::query_as::<_, OrderBookStateRow>(
        r#"
        SELECT engine_sequence, engine_timestamp_ms
        FROM orderbook_state
        WHERE market_id=$1
        "#,
    )
    .bind(market_id)
    .fetch_optional(pool())
    .await?;

    let bids = get_levels(market_id, "BID", depth).await?;
    let asks = get_levels(market_id, "ASK", depth).await?;

    Ok(OrderBookSnapshot {
        market_id,
        engine_sequence: state
            .as_ref()
            .map(|state| state.engine_sequence)
            .unwrap_or(0),
        engine_timestamp_ms: state
            .as_ref()
            .map(|state| state.engine_timestamp_ms)
            .unwrap_or(0),
        bids,
        asks,
    })
}

async fn get_levels(
    market_id: i64,
    side: &str,
    depth: i64,
) -> Result<Vec<OrderBookLevelRow>, sqlx::Error> {
    if side == "BID" {
        return sqlx::query_as::<_, OrderBookLevelRow>(
            r#"
            SELECT price, quantity
            FROM orderbook_levels
            WHERE market_id=$1 AND side='BID'
            ORDER BY price DESC
            LIMIT $2
            "#,
        )
        .bind(market_id)
        .bind(depth)
        .fetch_all(pool())
        .await;
    }

    sqlx::query_as::<_, OrderBookLevelRow>(
        r#"
        SELECT price, quantity
        FROM orderbook_levels
        WHERE market_id=$1 AND side='ASK'
        ORDER BY price ASC
        LIMIT $2
        "#,
    )
    .bind(market_id)
    .bind(depth)
    .fetch_all(pool())
    .await
}

#[derive(sqlx::FromRow)]
struct OrderBookStateRow {
    engine_sequence: i64,
    engine_timestamp_ms: i64,
}
