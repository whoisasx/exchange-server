use db::dto::{OrderStatus, OrderType as DbOrderType, SideType};
use protocol::{
    common::{OrderType, Side},
    engine::{
        OrderAccepted, OrderBookDelta, OrderBookLevel, OrderCancelled, OrderOpened, OrderRejected,
        ReservedPlaceOrder, TradeExecuted,
    },
};
use sqlx::{Pool, Postgres, Row};

#[derive(Debug)]
pub enum ProjectorRepositoryError {
    MissingOrderContext {
        reservation_id: Option<String>,
        order_id: Option<i64>,
    },
    Storage(sqlx::Error),
}

impl ProjectorRepositoryError {
    pub fn is_missing_order_context(&self) -> bool {
        matches!(self, Self::MissingOrderContext { .. })
    }
}

impl From<sqlx::Error> for ProjectorRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredOrderContext {
    reservation_id: Option<String>,
    request_id: String,
    order_id: Option<i64>,
    user_id: i64,
    market_id: i64,
    market_name: String,
    side: SideType,
    order_type: DbOrderType,
    quantity: i64,
    price: i64,
    margin: i64,
}

#[derive(Clone)]
pub struct ProjectorRepository {
    pool: Pool<Postgres>,
}

impl ProjectorRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn load_queue_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ProjectorRepositoryError> {
        let offset = sqlx::query(
            r#"
            SELECT next_offset
            FROM projector_offsets
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
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn save_order_context(
        &self,
        order: &ReservedPlaceOrder,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO projector_order_context(
                reservation_id,
                request_id,
                user_id,
                market_id,
                market_name,
                side,
                order_type,
                quantity,
                price
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT(reservation_id)
            DO UPDATE
            SET request_id=EXCLUDED.request_id,
                user_id=EXCLUDED.user_id,
                market_id=EXCLUDED.market_id,
                market_name=EXCLUDED.market_name,
                side=EXCLUDED.side,
                order_type=EXCLUDED.order_type,
                quantity=EXCLUDED.quantity,
                price=EXCLUDED.price,
                updated_at=NOW()
            "#,
        )
        .bind(&order.reservation_id)
        .bind(&order.envelope.request_id)
        .bind(order.envelope.user_id)
        .bind(order.market_id)
        .bind(&order.market_name)
        .bind(side_to_db(order.side))
        .bind(order_type_to_db(order.order_type))
        .bind(order.quantity)
        .bind(order.price)
        .execute(&mut *tx)
        .await?;

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_order_accepted(
        &self,
        reply: &OrderAccepted,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let mut context = load_context_by_reservation_in_tx(&mut tx, &reply.reservation_id).await?;
        context.order_id = Some(reply.order_id);

        sqlx::query(
            r#"
            UPDATE projector_order_context
            SET order_id=$2,
                status=CASE
                    WHEN status IN ('OPEN','PARTIAL','FILLED','CANCELLED') THEN status
                    ELSE 'ACCEPTED'
                END,
                reject_reason=NULL,
                updated_at=NOW()
            WHERE reservation_id=$1
            "#,
        )
        .bind(&reply.reservation_id)
        .bind(reply.order_id)
        .execute(&mut *tx)
        .await?;

        insert_order_if_missing_in_tx(&mut tx, &context, reply.order_id).await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_order_rejected(
        &self,
        reply: &OrderRejected,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;

        if let Some(reservation_id) = &reply.reservation_id {
            let _ = load_context_by_reservation_in_tx(&mut tx, reservation_id).await?;

            sqlx::query(
                r#"
                UPDATE projector_order_context
                SET status=CASE
                        WHEN status IN ('ACCEPTED','OPEN','PARTIAL','FILLED','CANCELLED') THEN status
                        ELSE 'REJECTED'
                    END,
                    reject_reason=$2,
                    updated_at=NOW()
                WHERE reservation_id=$1
                "#,
            )
            .bind(reservation_id)
            .bind(&reply.reason)
            .execute(&mut *tx)
            .await?;
        }

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn project_order_opened(
        &self,
        event: &OrderOpened,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let mut context = load_context_by_reservation_in_tx(&mut tx, &event.reservation_id).await?;
        context.order_id = Some(event.order_id);

        sqlx::query(
            r#"
            UPDATE projector_order_context
            SET order_id=$2,
                status=CASE
                    WHEN status IN ('PARTIAL','FILLED','CANCELLED') THEN status
                    ELSE 'OPEN'
                END,
                updated_at=NOW()
            WHERE reservation_id=$1
            "#,
        )
        .bind(&event.reservation_id)
        .bind(event.order_id)
        .execute(&mut *tx)
        .await?;

        upsert_order_open_in_tx(&mut tx, &context, event.order_id).await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn project_order_cancelled(
        &self,
        event: &OrderCancelled,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let mut context = load_context_for_order_in_tx(
            &mut tx,
            Some(event.reservation_id.as_str()),
            event.order_id,
        )
        .await?;
        context.order_id = Some(event.order_id);

        sqlx::query(
            r#"
            UPDATE projector_order_context
            SET order_id=COALESCE(order_id, $2),
                status='CANCELLED',
                updated_at=NOW()
            WHERE reservation_id=$1
            "#,
        )
        .bind(&event.reservation_id)
        .bind(event.order_id)
        .execute(&mut *tx)
        .await?;

        upsert_order_cancelled_in_tx(&mut tx, &context, event.order_id).await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn project_trade_executed(
        &self,
        event: &TradeExecuted,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let mut maker = load_context_for_order_in_tx(
            &mut tx,
            event.maker_reservation_id.as_deref(),
            event.maker_order_id,
        )
        .await?;
        let mut taker = load_context_for_order_in_tx(
            &mut tx,
            event.taker_reservation_id.as_deref(),
            event.taker_order_id,
        )
        .await?;
        maker.order_id = Some(event.maker_order_id);
        taker.order_id = Some(event.taker_order_id);

        insert_order_if_missing_in_tx(&mut tx, &maker, event.maker_order_id).await?;
        insert_order_if_missing_in_tx(&mut tx, &taker, event.taker_order_id).await?;
        update_context_order_id_in_tx(
            &mut tx,
            maker.reservation_id.as_deref(),
            event.maker_order_id,
        )
        .await?;
        update_context_order_id_in_tx(
            &mut tx,
            taker.reservation_id.as_deref(),
            event.taker_order_id,
        )
        .await?;

        sqlx::query(
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
                executed_at
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,TO_TIMESTAMP($12::DOUBLE PRECISION / 1000.0))
            ON CONFLICT(fill_id) DO NOTHING
            "#,
        )
        .bind(event.fill_id)
        .bind(event.market_id)
        .bind(event.engine_sequence)
        .bind(maker.user_id)
        .bind(taker.user_id)
        .bind(event.maker_order_id)
        .bind(event.taker_order_id)
        .bind(event.price)
        .bind(event.quantity)
        .bind(maker.side)
        .bind(taker.side)
        .bind(event.engine_timestamp_ms)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE markets
            SET last_traded_price=$1
            WHERE market_id=$2
            "#,
        )
        .bind(event.price)
        .bind(event.market_id)
        .execute(&mut *tx)
        .await?;

        update_order_after_fill_in_tx(&mut tx, &maker, event.maker_order_id).await?;
        update_order_after_fill_in_tx(&mut tx, &taker, event.taker_order_id).await?;
        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn project_orderbook_delta(
        &self,
        event: &OrderBookDelta,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO orderbook_events(
                market_id,
                engine_sequence,
                engine_timestamp_ms,
                topic,
                partition,
                offset_value
            )
            VALUES($1,$2,$3,$4,$5,$6)
            ON CONFLICT(market_id, engine_sequence) DO NOTHING
            RETURNING engine_sequence
            "#,
        )
        .bind(event.market_id)
        .bind(event.engine_sequence)
        .bind(event.engine_timestamp_ms)
        .bind(topic)
        .bind(partition)
        .bind(next_offset - 1)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();

        if inserted && should_apply_orderbook_delta_in_tx(&mut tx, event).await? {
            for level in &event.bids {
                apply_orderbook_level_in_tx(&mut tx, event, "BID", level).await?;
            }
            for level in &event.asks {
                apply_orderbook_level_in_tx(&mut tx, event, "ASK", level).await?;
            }
            upsert_orderbook_state_in_tx(&mut tx, event).await?;
        }

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }
}

async fn save_queue_offset_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO projector_offsets(topic, partition, next_offset)
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
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn should_apply_orderbook_delta_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: &OrderBookDelta,
) -> Result<bool, ProjectorRepositoryError> {
    let current_sequence = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT engine_sequence
        FROM orderbook_state
        WHERE market_id=$1
        "#,
    )
    .bind(event.market_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(current_sequence
        .map(|sequence| event.engine_sequence > sequence)
        .unwrap_or(true))
}

async fn upsert_orderbook_state_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: &OrderBookDelta,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO orderbook_state(
            market_id,
            engine_sequence,
            engine_timestamp_ms
        )
        VALUES($1,$2,$3)
        ON CONFLICT(market_id)
        DO UPDATE
        SET engine_sequence=EXCLUDED.engine_sequence,
            engine_timestamp_ms=EXCLUDED.engine_timestamp_ms,
            updated_at=NOW()
        WHERE orderbook_state.engine_sequence < EXCLUDED.engine_sequence
        "#,
    )
    .bind(event.market_id)
    .bind(event.engine_sequence)
    .bind(event.engine_timestamp_ms)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn apply_orderbook_level_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: &OrderBookDelta,
    side: &str,
    level: &OrderBookLevel,
) -> Result<(), ProjectorRepositoryError> {
    if level.quantity <= 0 {
        sqlx::query(
            r#"
            DELETE FROM orderbook_levels
            WHERE market_id=$1
              AND side=$2
              AND price=$3
              AND last_engine_sequence < $4
            "#,
        )
        .bind(event.market_id)
        .bind(side)
        .bind(level.price)
        .bind(event.engine_sequence)
        .execute(&mut **tx)
        .await?;
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO orderbook_levels(
            market_id,
            side,
            price,
            quantity,
            last_engine_sequence
        )
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT(market_id, side, price)
        DO UPDATE
        SET quantity=EXCLUDED.quantity,
            last_engine_sequence=EXCLUDED.last_engine_sequence,
            updated_at=NOW()
        WHERE orderbook_levels.last_engine_sequence < EXCLUDED.last_engine_sequence
        "#,
    )
    .bind(event.market_id)
    .bind(side)
    .bind(level.price)
    .bind(level.quantity)
    .bind(event.engine_sequence)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn load_context_for_order_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: Option<&str>,
    order_id: i64,
) -> Result<StoredOrderContext, ProjectorRepositoryError> {
    if let Some(reservation_id) = reservation_id {
        if let Some(context) = maybe_context_by_reservation_in_tx(tx, reservation_id).await? {
            return Ok(context);
        }
    }

    if let Some(context) = maybe_context_by_order_id_in_tx(tx, order_id).await? {
        return Ok(context);
    }

    Err(ProjectorRepositoryError::MissingOrderContext {
        reservation_id: reservation_id.map(String::from),
        order_id: Some(order_id),
    })
}

async fn load_context_by_reservation_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: &str,
) -> Result<StoredOrderContext, ProjectorRepositoryError> {
    match maybe_context_by_reservation_in_tx(tx, reservation_id).await? {
        Some(context) => Ok(context),
        None => Err(ProjectorRepositoryError::MissingOrderContext {
            reservation_id: Some(String::from(reservation_id)),
            order_id: None,
        }),
    }
}

async fn maybe_context_by_reservation_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: &str,
) -> Result<Option<StoredOrderContext>, ProjectorRepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT
            c.reservation_id,
            c.request_id,
            c.order_id,
            c.user_id,
            c.market_id,
            c.market_name,
            c.side,
            c.order_type,
            c.quantity,
            c.price,
            COALESCE(w.amount, 0) AS margin
        FROM projector_order_context c
        LEFT JOIN wallet_reservations w ON w.reservation_id=c.reservation_id
        WHERE c.reservation_id=$1
        "#,
    )
    .bind(reservation_id)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(context_from_row).transpose()
}

async fn maybe_context_by_order_id_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    order_id: i64,
) -> Result<Option<StoredOrderContext>, ProjectorRepositoryError> {
    if let Some(row) = sqlx::query(
        r#"
        SELECT
            c.reservation_id,
            c.request_id,
            c.order_id,
            c.user_id,
            c.market_id,
            c.market_name,
            c.side,
            c.order_type,
            c.quantity,
            c.price,
            COALESCE(w.amount, 0) AS margin
        FROM projector_order_context c
        LEFT JOIN wallet_reservations w ON w.reservation_id=c.reservation_id
        WHERE c.order_id=$1
        "#,
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        return context_from_row(row).map(Some);
    }

    let row = sqlx::query(
        r#"
        SELECT
            NULL::TEXT AS reservation_id,
            ''::TEXT AS request_id,
            order_id,
            user_id,
            market_id,
            market_name,
            side,
            order_type,
            quantity,
            price,
            margin
        FROM orders
        WHERE order_id=$1
        "#,
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(context_from_row).transpose()
}

fn context_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<StoredOrderContext, ProjectorRepositoryError> {
    Ok(StoredOrderContext {
        reservation_id: row.get("reservation_id"),
        request_id: row.get("request_id"),
        order_id: row.get("order_id"),
        user_id: row.get("user_id"),
        market_id: row.get("market_id"),
        market_name: row.get("market_name"),
        side: row.get("side"),
        order_type: row.get("order_type"),
        quantity: row.get("quantity"),
        price: row.get("price"),
        margin: row.get("margin"),
    })
}

async fn update_context_order_id_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: Option<&str>,
    order_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    if let Some(reservation_id) = reservation_id {
        sqlx::query(
            r#"
            UPDATE projector_order_context
            SET order_id=COALESCE(order_id, $2),
                updated_at=NOW()
            WHERE reservation_id=$1
            "#,
        )
        .bind(reservation_id)
        .bind(order_id)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn insert_order_if_missing_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO orders(
            order_id,
            user_id,
            market_id,
            market_name,
            side,
            order_type,
            quantity,
            price,
            status,
            margin
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(order_id) DO NOTHING
        "#,
    )
    .bind(order_id)
    .bind(context.user_id)
    .bind(context.market_id)
    .bind(&context.market_name)
    .bind(context.side)
    .bind(context.order_type)
    .bind(context.quantity)
    .bind(context.price)
    .bind(OrderStatus::PENDING)
    .bind(context.margin)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn upsert_order_open_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO orders(
            order_id,
            user_id,
            market_id,
            market_name,
            side,
            order_type,
            quantity,
            price,
            status,
            margin
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(order_id)
        DO UPDATE
        SET status=CASE
                WHEN orders.status IN ('FILLED','CANCELLED') THEN orders.status
                ELSE EXCLUDED.status
            END,
            updated_at=NOW()
        "#,
    )
    .bind(order_id)
    .bind(context.user_id)
    .bind(context.market_id)
    .bind(&context.market_name)
    .bind(context.side)
    .bind(context.order_type)
    .bind(context.quantity)
    .bind(context.price)
    .bind(OrderStatus::OPEN)
    .bind(context.margin)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn upsert_order_cancelled_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO orders(
            order_id,
            user_id,
            market_id,
            market_name,
            side,
            order_type,
            quantity,
            price,
            status,
            margin
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(order_id)
        DO UPDATE
        SET status=EXCLUDED.status,
            updated_at=NOW()
        "#,
    )
    .bind(order_id)
    .bind(context.user_id)
    .bind(context.market_id)
    .bind(&context.market_name)
    .bind(context.side)
    .bind(context.order_type)
    .bind(context.quantity)
    .bind(context.price)
    .bind(OrderStatus::CANCELLED)
    .bind(context.margin)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn update_order_after_fill_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    let filled_quantity = cumulative_filled_quantity_in_tx(tx, order_id).await?;
    let status = order_status_after_fill(context.quantity, filled_quantity);
    let context_status = context_status_for_order_status(status);

    sqlx::query(
        r#"
        UPDATE orders
        SET status=$2,
            updated_at=NOW()
        WHERE order_id=$1 AND status <> 'CANCELLED'
        "#,
    )
    .bind(order_id)
    .bind(status)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE projector_order_context
        SET status=CASE
                WHEN status='CANCELLED' THEN status
                ELSE $2
            END,
            updated_at=NOW()
        WHERE order_id=$1 OR ($3::TEXT IS NOT NULL AND reservation_id=$3)
        "#,
    )
    .bind(order_id)
    .bind(context_status)
    .bind(context.reservation_id.as_deref())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn cumulative_filled_quantity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    order_id: i64,
) -> Result<i64, ProjectorRepositoryError> {
    let filled_quantity = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(SUM(quantity), 0)::BIGINT
        FROM fills
        WHERE maker_order_id=$1 OR taker_order_id=$1
        "#,
    )
    .bind(order_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(filled_quantity)
}

fn side_to_db(side: Side) -> SideType {
    match side {
        Side::LONG => SideType::LONG,
        Side::SHORT => SideType::SHORT,
    }
}

fn order_type_to_db(order_type: OrderType) -> DbOrderType {
    match order_type {
        OrderType::LIMIT => DbOrderType::LIMIT,
        OrderType::MARKET => DbOrderType::MARKET,
    }
}

pub(crate) fn order_status_after_fill(order_quantity: i64, filled_quantity: i64) -> OrderStatus {
    if filled_quantity >= order_quantity {
        OrderStatus::FILLED
    } else {
        OrderStatus::PARTIAL
    }
}

fn context_status_for_order_status(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::PENDING => "PENDING",
        OrderStatus::OPEN => "OPEN",
        OrderStatus::FILLED => "FILLED",
        OrderStatus::PARTIAL => "PARTIAL",
        OrderStatus::CANCELLED => "CANCELLED",
        OrderStatus::REJECTED => "REJECTED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_status_after_fill_marks_partial_before_target_quantity() {
        assert_eq!(order_status_after_fill(10, 4), OrderStatus::PARTIAL);
    }

    #[test]
    fn order_status_after_fill_marks_filled_at_or_above_target_quantity() {
        assert_eq!(order_status_after_fill(10, 10), OrderStatus::FILLED);
        assert_eq!(order_status_after_fill(10, 12), OrderStatus::FILLED);
    }

    #[test]
    fn protocol_side_mapping_matches_db_names() {
        assert_eq!(side_to_db(Side::LONG), SideType::LONG);
        assert_eq!(side_to_db(Side::SHORT), SideType::SHORT);
    }

    #[test]
    fn protocol_order_type_mapping_matches_db_names() {
        assert_eq!(order_type_to_db(OrderType::LIMIT), DbOrderType::LIMIT);
        assert_eq!(order_type_to_db(OrderType::MARKET), DbOrderType::MARKET);
    }
}
