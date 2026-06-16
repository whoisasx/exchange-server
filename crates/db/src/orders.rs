use chrono::{DateTime, Utc};

use crate::{
    dto::{OrderRow, OrderStatus, OrderType, SideType},
    pool::pool,
};

pub async fn create_order(
    order_id: i64,
    user_id: i64,
    market_id: i64,
    market_name: &str,
    side: SideType,
    order_type: OrderType,
    quantity: i64,
    price: i64,
    status: OrderStatus,
    margin: i64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> Result<OrderRow, sqlx::Error> {
    let new_order = sqlx::query_as!(
        OrderRow,
        r#"
        INSERT INTO orders(order_id, user_id, market_id, market_name, side, order_type, quantity, price, status, margin, created_at, updated_at)
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING
            order_id,
            user_id,
            market_id,
            market_name,
            side as "side!: SideType",
            order_type as "order_type!: OrderType",
            quantity,
            price,
            status as "status!: OrderStatus",
            margin,
            created_at,
            updated_at
        "#,
        order_id,
        user_id,
        market_id,
        market_name,
        side as SideType,
        order_type as OrderType,
        quantity,
        price,
        status as OrderStatus,
        margin,
        created_at,
        updated_at
    )
    .fetch_one(pool())
    .await?;

    Ok(new_order)
}

pub async fn get_order_by_order_id(order_id: i64) -> Result<OrderRow, sqlx::Error> {
    let order = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE order_id=$1
    "#,
        order_id
    )
    .fetch_one(pool())
    .await?;

    Ok(order)
}

pub async fn get_orders_by_user_id(user_id: i64) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE user_id=$1
            ORDER BY created_at DESC
        "#,
        user_id
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}

pub async fn get_market_orders(user_id: i64, market_id: i64) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE user_id=$1 AND market_id=$2
            ORDER BY created_at DESC
        "#,
        user_id,
        market_id
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}

pub async fn get_orders_by_market_id(market_id: i64) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE market_id=$1
            ORDER BY created_at DESC
        "#,
        market_id
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}

pub async fn get_orders_by_market_status(
    market_id: i64,
    status: OrderStatus,
) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE market_id=$1 AND status=$2
            ORDER BY created_at DESC
        "#,
        market_id,
        status as OrderStatus
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}

pub async fn get_user_market_orders_by_status(
    user_id: i64,
    market_id: i64,
    status: OrderStatus,
) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE user_id=$1 AND market_id=$2 AND status=$3
            ORDER BY created_at DESC
        "#,
        user_id,
        market_id,
        status as OrderStatus
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}

pub async fn update_order_status(
    order_id: i64,
    status: OrderStatus,
    updated_at: DateTime<Utc>,
) -> Result<OrderRow, sqlx::Error> {
    let order = sqlx::query_as!(
        OrderRow,
        r#"
            UPDATE orders
            SET status=$2,
                updated_at=$3
            WHERE order_id=$1
            RETURNING
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
        "#,
        order_id,
        status as OrderStatus,
        updated_at
    )
    .fetch_one(pool())
    .await?;

    Ok(order)
}

pub async fn cancel_order(
    order_id: i64,
    updated_at: DateTime<Utc>,
) -> Result<OrderRow, sqlx::Error> {
    update_order_status(order_id, OrderStatus::CANCELLED, updated_at).await
}

pub async fn get_open_market_orders(market_id: i64) -> Result<Vec<OrderRow>, sqlx::Error> {
    let orders = sqlx::query_as!(
        OrderRow,
        r#"
            SELECT
                order_id,
                user_id,
                market_id,
                market_name,
                side as "side!: SideType",
                order_type as "order_type!: OrderType",
                quantity,
                price,
                status as "status!: OrderStatus",
                margin,
                created_at,
                updated_at
            FROM orders
            WHERE market_id=$1 AND status= $2
            ORDER BY created_at DESC
        "#,
        market_id,
        OrderStatus::OPEN as OrderStatus,
    )
    .fetch_all(pool())
    .await?;

    Ok(orders)
}
