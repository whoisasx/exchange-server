use chrono::{DateTime, Utc};
use db::dto::{CloseType, MarginType, OrderStatus, OrderType as DbOrderType, SideType};
use protocol::{
    common::{OrderType, Side},
    engine::{
        EngineEvent, ExecutionReason, LiquidatePosition, LiquidationAccepted, LiquidationRejected,
        OrderAccepted, OrderBookDelta, OrderBookLevel, OrderCancelled, OrderOpened, OrderRejected,
        ReservedPlaceOrder, TradeExecuted,
    },
};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};

const SAVE_QUEUE_OFFSET_SQL: &str = r#"
INSERT INTO projector_offsets(topic, partition, next_offset)
VALUES($1,$2,$3)
ON CONFLICT(topic, partition)
DO UPDATE
SET next_offset=EXCLUDED.next_offset,
    updated_at=NOW()
WHERE projector_offsets.next_offset < EXCLUDED.next_offset
"#;

#[derive(Debug)]
pub enum ProjectorRepositoryError {
    MissingOrderContext {
        reservation_id: Option<String>,
        order_id: Option<i64>,
    },
    Serialization(serde_json::Error),
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

impl From<serde_json::Error> for ProjectorRepositoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
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
    reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPosition {
    position_id: i64,
    side: SideType,
    quantity: i64,
    initial_margin: i64,
    average_price: i64,
    opened_at: DateTime<Utc>,
    open_order_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct EngineEventLogEntry {
    engine_event_id: String,
    event_type: &'static str,
    market_id: Option<i64>,
    engine_sequence: Option<i64>,
    engine_timestamp_ms: i64,
    payload: Value,
}

#[derive(Debug, Clone, Copy)]
struct EngineEventLogMetadata<'a> {
    event_type: &'static str,
    engine_event_id: Option<&'a str>,
    market_id: Option<i64>,
    engine_sequence: Option<i64>,
    engine_timestamp_ms: i64,
    checkpoint_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeParticipant {
    Maker,
    Taker,
}

impl TradeParticipant {
    fn label(self) -> &'static str {
        match self {
            Self::Maker => "maker",
            Self::Taker => "taker",
        }
    }

    fn order_id(self, event: &TradeExecuted) -> i64 {
        match self {
            Self::Maker => event.maker_order_id,
            Self::Taker => event.taker_order_id,
        }
    }

    fn user_id(self, event: &TradeExecuted) -> i64 {
        match self {
            Self::Maker => event.maker_user_id,
            Self::Taker => event.taker_user_id,
        }
    }

    fn reservation_id<'a>(self, event: &'a TradeExecuted) -> Option<&'a str> {
        match self {
            Self::Maker => event.maker_reservation_id.as_deref(),
            Self::Taker => event.taker_reservation_id.as_deref(),
        }
    }
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
                price,
                reduce_only
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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
                reduce_only=EXCLUDED.reduce_only,
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
        .bind(order.reduce_only)
        .execute(&mut *tx)
        .await?;

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn save_liquidation_context(
        &self,
        liquidation: &LiquidatePosition,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let close_side = opposite_side(liquidation.position_side);
        let order_type = if liquidation.price == 0 {
            DbOrderType::MARKET
        } else {
            DbOrderType::LIMIT
        };

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
                price,
                reduce_only
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,true)
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
                reduce_only=EXCLUDED.reduce_only,
                updated_at=NOW()
            "#,
        )
        .bind(&liquidation.liquidation_id)
        .bind(&liquidation.envelope.request_id)
        .bind(liquidation.liquidated_user_id)
        .bind(liquidation.market_id)
        .bind(&liquidation.market_name)
        .bind(close_side)
        .bind(order_type)
        .bind(liquidation.quantity)
        .bind(liquidation.price)
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
        let _ = load_context_by_reservation_in_tx(&mut tx, &reply.reservation_id).await?;

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

    pub async fn mark_liquidation_accepted(
        &self,
        reply: &LiquidationAccepted,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let _ = load_context_by_reservation_in_tx(&mut tx, &reply.liquidation_id).await?;

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
        .bind(&reply.liquidation_id)
        .bind(reply.order_id)
        .execute(&mut *tx)
        .await?;

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn mark_liquidation_rejected(
        &self,
        reply: &LiquidationRejected,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let mut tx = self.pool.begin().await?;
        let _ = load_context_by_reservation_in_tx(&mut tx, &reply.liquidation_id).await?;

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
        .bind(&reply.liquidation_id)
        .bind(&reply.reason)
        .execute(&mut *tx)
        .await?;

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
        let (mut maker, mut taker) = load_trade_contexts_in_tx(&mut tx, event).await?;
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

        let inserted_fill = sqlx::query_scalar::<_, i64>(
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
            ON CONFLICT DO NOTHING
            RETURNING fill_id
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
        .fetch_optional(&mut *tx)
        .await?
        .is_some();

        update_order_after_fill_in_tx(&mut tx, &maker, event.maker_order_id).await?;
        update_order_after_fill_in_tx(&mut tx, &taker, event.taker_order_id).await?;
        if inserted_fill {
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

            project_position_for_fill_in_tx(
                &mut tx,
                &maker,
                event.maker_order_id,
                event.fill_id,
                event.price,
                event.quantity,
                event.engine_timestamp_ms,
                event.execution_reason,
            )
            .await?;
            project_position_for_fill_in_tx(
                &mut tx,
                &taker,
                event.taker_order_id,
                event.fill_id,
                event.price,
                event.quantity,
                event.engine_timestamp_ms,
                event.execution_reason,
            )
            .await?;
            refresh_position_pnl_for_market_in_tx(
                &mut tx,
                event.market_id,
                event.price,
                event.engine_timestamp_ms,
            )
            .await?;
        }
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

    pub async fn project_engine_event_log(
        &self,
        event: &EngineEvent,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), ProjectorRepositoryError> {
        let entry = engine_event_log_entry(event)?;
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO engine_event_log(
                engine_event_id,
                event_type,
                market_id,
                engine_sequence,
                engine_timestamp_ms,
                topic,
                partition,
                offset_value,
                payload
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT(engine_event_id) DO NOTHING
            "#,
        )
        .bind(&entry.engine_event_id)
        .bind(entry.event_type)
        .bind(entry.market_id)
        .bind(entry.engine_sequence)
        .bind(entry.engine_timestamp_ms)
        .bind(topic)
        .bind(partition)
        .bind(next_offset - 1)
        .bind(&entry.payload)
        .execute(&mut *tx)
        .await?;

        save_queue_offset_in_tx(&mut tx, topic, partition, next_offset).await?;
        tx.commit().await?;

        Ok(())
    }
}

fn engine_event_log_entry(
    event: &EngineEvent,
) -> Result<EngineEventLogEntry, ProjectorRepositoryError> {
    let metadata = engine_event_log_metadata(event);
    let payload = serde_json::to_value(event)?;

    Ok(EngineEventLogEntry {
        engine_event_id: engine_event_log_id(metadata),
        event_type: metadata.event_type,
        market_id: metadata.market_id,
        engine_sequence: metadata.engine_sequence,
        engine_timestamp_ms: metadata.engine_timestamp_ms,
        payload,
    })
}

fn engine_event_log_id(metadata: EngineEventLogMetadata<'_>) -> String {
    if let Some(engine_event_id) = metadata.engine_event_id.filter(|id| !id.is_empty()) {
        return engine_event_id.to_owned();
    }

    if let Some(checkpoint_id) = metadata.checkpoint_id.filter(|id| !id.is_empty()) {
        return format!("checkpoint:{checkpoint_id}");
    }

    match (metadata.market_id, metadata.engine_sequence) {
        (Some(market_id), Some(engine_sequence)) => {
            format!("{}:{market_id}:{engine_sequence}", metadata.event_type)
        }
        _ => format!("{}:{}", metadata.event_type, metadata.engine_timestamp_ms),
    }
}

fn engine_event_log_metadata(event: &EngineEvent) -> EngineEventLogMetadata<'_> {
    match event {
        EngineEvent::OrderOpened(event) => EngineEventLogMetadata {
            event_type: "OrderOpened",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::OrderCancelled(event) => EngineEventLogMetadata {
            event_type: "OrderCancelled",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::OrderExpired(event) => EngineEventLogMetadata {
            event_type: "OrderExpired",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::ReservationReleased(event) => EngineEventLogMetadata {
            event_type: "ReservationReleased",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::TradeExecuted(event) => EngineEventLogMetadata {
            event_type: "TradeExecuted",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::OrderBookDelta(event) => EngineEventLogMetadata {
            event_type: "OrderBookDelta",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::MarkPriceUpdated(event) => EngineEventLogMetadata {
            event_type: "MarkPriceUpdated",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::FundingRateUpdated(event) => EngineEventLogMetadata {
            event_type: "FundingRateUpdated",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::FundingPaymentApplied(event) => EngineEventLogMetadata {
            event_type: "FundingPaymentApplied",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::PositionChanged(event) => EngineEventLogMetadata {
            event_type: "PositionChanged",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::RiskStateUpdated(event) => EngineEventLogMetadata {
            event_type: "RiskStateUpdated",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::LiquidationStarted(event) => EngineEventLogMetadata {
            event_type: "LiquidationStarted",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::LiquidationExecuted(event) => EngineEventLogMetadata {
            event_type: "LiquidationExecuted",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::LiquidationCompleted(event) => EngineEventLogMetadata {
            event_type: "LiquidationCompleted",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::AdlExecuted(event) => EngineEventLogMetadata {
            event_type: "AdlExecuted",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
        EngineEvent::AccountDelta(event) => EngineEventLogMetadata {
            event_type: "AccountDelta",
            engine_event_id: event.engine_event_id.as_deref(),
            market_id: Some(event.market_id),
            engine_sequence: Some(event.engine_sequence),
            engine_timestamp_ms: event.engine_timestamp_ms,
            checkpoint_id: None,
        },
    }
}

async fn save_queue_offset_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(SAVE_QUEUE_OFFSET_SQL)
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

async fn project_position_for_fill_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
    fill_id: i64,
    price: i64,
    quantity: i64,
    engine_timestamp_ms: i64,
    execution_reason: ExecutionReason,
) -> Result<(), ProjectorRepositoryError> {
    let position = load_position_for_update_in_tx(tx, context.user_id, context.market_id).await?;

    match position {
        None if context.reduce_only => Ok(()),
        None => {
            let initial_margin = proportional_margin(context.margin, context.quantity, quantity);
            insert_open_position_in_tx(
                tx,
                context,
                order_id,
                fill_id,
                quantity,
                price,
                initial_margin,
                engine_timestamp_ms,
            )
            .await
        }
        Some(position) if context.reduce_only && position.side == context.side => Ok(()),
        Some(position) if position.side == context.side => {
            let added_margin = proportional_margin(context.margin, context.quantity, quantity);
            increase_position_in_tx(tx, &position, fill_id, quantity, price, added_margin).await
        }
        Some(position) => {
            reduce_or_reverse_position_in_tx(
                tx,
                &position,
                context,
                order_id,
                fill_id,
                price,
                quantity,
                engine_timestamp_ms,
                execution_reason,
            )
            .await
        }
    }
}

async fn refresh_position_pnl_for_market_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    market_id: i64,
    mark_price: i64,
    engine_timestamp_ms: i64,
) -> Result<(), ProjectorRepositoryError> {
    let rows = sqlx::query(
        r#"
        SELECT
            position_id,
            side,
            quantity,
            average_price
        FROM positions
        WHERE market_id=$1
        FOR UPDATE
        "#,
    )
    .bind(market_id)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let unrealized_pnl = unrealized_pnl(
            row.get("side"),
            row.get("average_price"),
            mark_price,
            row.get("quantity"),
        );

        sqlx::query(
            r#"
            UPDATE positions
            SET unrealized_pnl=$2,
                updated_at=TO_TIMESTAMP($3::DOUBLE PRECISION / 1000.0)
            WHERE position_id=$1
            "#,
        )
        .bind(row.get::<i64, _>("position_id"))
        .bind(unrealized_pnl)
        .bind(engine_timestamp_ms)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn load_position_for_update_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    user_id: i64,
    market_id: i64,
) -> Result<Option<StoredPosition>, ProjectorRepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT
            position_id,
            side,
            quantity,
            initial_margin,
            average_price,
            opened_at,
            open_order_id
        FROM positions
        WHERE user_id=$1 AND market_id=$2
        FOR UPDATE
        "#,
    )
    .bind(user_id)
    .bind(market_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.map(|row| StoredPosition {
        position_id: row.get("position_id"),
        side: row.get("side"),
        quantity: row.get("quantity"),
        initial_margin: row.get("initial_margin"),
        average_price: row.get("average_price"),
        opened_at: row.get("opened_at"),
        open_order_id: row.get("open_order_id"),
    }))
}

async fn insert_open_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    context: &StoredOrderContext,
    order_id: i64,
    fill_id: i64,
    quantity: i64,
    price: i64,
    initial_margin: i64,
    engine_timestamp_ms: i64,
) -> Result<(), ProjectorRepositoryError> {
    let position_id = next_projector_position_id_in_tx(tx).await?;

    sqlx::query(
        r#"
        INSERT INTO positions(
            position_id,
            user_id,
            market_id,
            market_name,
            side,
            quantity,
            unrealized_pnl,
            initial_margin,
            maintenance_margin,
            margin_chosen,
            liquidation_price,
            average_price,
            opened_at,
            updated_at,
            open_order_id
        )
        VALUES(
            $1,$2,$3,$4,$5,$6,0,$7,0,$8,0,$9,
            TO_TIMESTAMP($10::DOUBLE PRECISION / 1000.0),
            TO_TIMESTAMP($10::DOUBLE PRECISION / 1000.0),
            $11
        )
        "#,
    )
    .bind(position_id)
    .bind(context.user_id)
    .bind(context.market_id)
    .bind(&context.market_name)
    .bind(context.side)
    .bind(quantity)
    .bind(initial_margin)
    .bind(MarginType::ISOLATED)
    .bind(price)
    .bind(engine_timestamp_ms)
    .bind(order_id)
    .execute(&mut **tx)
    .await?;

    link_fill_to_open_position_in_tx(tx, position_id, fill_id).await
}

async fn increase_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position: &StoredPosition,
    fill_id: i64,
    quantity: i64,
    price: i64,
    added_margin: i64,
) -> Result<(), ProjectorRepositoryError> {
    let next_quantity = position.quantity + quantity;
    let next_average_price =
        weighted_average_price(position.average_price, position.quantity, price, quantity);

    sqlx::query(
        r#"
        UPDATE positions
        SET quantity=$2,
            initial_margin=$3,
            average_price=$4,
            updated_at=NOW()
        WHERE position_id=$1
        "#,
    )
    .bind(position.position_id)
    .bind(next_quantity)
    .bind(position.initial_margin + added_margin)
    .bind(next_average_price)
    .execute(&mut **tx)
    .await?;

    link_fill_to_open_position_in_tx(tx, position.position_id, fill_id).await
}

async fn reduce_or_reverse_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position: &StoredPosition,
    context: &StoredOrderContext,
    order_id: i64,
    fill_id: i64,
    price: i64,
    quantity: i64,
    engine_timestamp_ms: i64,
    execution_reason: ExecutionReason,
) -> Result<(), ProjectorRepositoryError> {
    let closed_quantity = position.quantity.min(quantity);
    let closed_margin =
        proportional_margin(position.initial_margin, position.quantity, closed_quantity);

    let closed_position_id = insert_closed_position_in_tx(
        tx,
        position,
        context,
        order_id,
        fill_id,
        closed_quantity,
        price,
        closed_margin,
        engine_timestamp_ms,
        close_type_from_execution_reason(execution_reason),
    )
    .await?;

    if closed_quantity < position.quantity {
        let remaining_quantity = position.quantity - closed_quantity;
        let remaining_margin = (position.initial_margin - closed_margin).max(0);

        sqlx::query(
            r#"
            UPDATE positions
            SET quantity=$2,
                initial_margin=$3,
                updated_at=TO_TIMESTAMP($4::DOUBLE PRECISION / 1000.0)
            WHERE position_id=$1
            "#,
        )
        .bind(position.position_id)
        .bind(remaining_quantity)
        .bind(remaining_margin)
        .bind(engine_timestamp_ms)
        .execute(&mut **tx)
        .await?;

        return Ok(());
    }

    transfer_open_position_fills_to_closed_in_tx(tx, position.position_id, closed_position_id)
        .await?;
    delete_open_position_in_tx(tx, position.position_id).await?;

    let reversal_quantity = quantity - closed_quantity;
    if reversal_quantity > 0 {
        let reversal_margin =
            proportional_margin(context.margin, context.quantity, reversal_quantity);
        insert_open_position_in_tx(
            tx,
            context,
            order_id,
            fill_id,
            reversal_quantity,
            price,
            reversal_margin,
            engine_timestamp_ms,
        )
        .await?;
    }

    Ok(())
}

async fn insert_closed_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position: &StoredPosition,
    context: &StoredOrderContext,
    close_order_id: i64,
    fill_id: i64,
    quantity: i64,
    exit_price: i64,
    initial_margin: i64,
    engine_timestamp_ms: i64,
    close_reason: CloseType,
) -> Result<i64, ProjectorRepositoryError> {
    let closed_position_id = next_projector_position_id_in_tx(tx).await?;
    let open_order_id = position.open_order_id.unwrap_or(close_order_id);
    let realized_pnl = realized_pnl(position.side, position.average_price, exit_price, quantity);

    sqlx::query(
        r#"
        INSERT INTO closed_positions(
            position_id,
            user_id,
            market_id,
            market_name,
            side,
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
            close_reason
        )
        VALUES(
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,$11,
            TO_TIMESTAMP($12::DOUBLE PRECISION / 1000.0),
            $13,$14,$15
        )
        "#,
    )
    .bind(closed_position_id)
    .bind(context.user_id)
    .bind(context.market_id)
    .bind(&context.market_name)
    .bind(position.side)
    .bind(quantity)
    .bind(position.average_price)
    .bind(exit_price)
    .bind(realized_pnl)
    .bind(initial_margin)
    .bind(position.opened_at)
    .bind(engine_timestamp_ms)
    .bind(open_order_id)
    .bind(close_order_id)
    .bind(close_reason)
    .execute(&mut **tx)
    .await?;

    link_fill_to_closed_position_in_tx(tx, closed_position_id, fill_id).await?;

    Ok(closed_position_id)
}

async fn transfer_open_position_fills_to_closed_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    open_position_id: i64,
    closed_position_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO closed_position_fills(position_id, fill_id)
        SELECT $1, fill_id
        FROM position_fills
        WHERE position_id=$2
        ON CONFLICT(position_id, fill_id) DO NOTHING
        "#,
    )
    .bind(closed_position_id)
    .bind(open_position_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn delete_open_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        DELETE FROM positions
        WHERE position_id=$1
        "#,
    )
    .bind(position_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn link_fill_to_open_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position_id: i64,
    fill_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO position_fills(position_id, fill_id)
        VALUES($1,$2)
        ON CONFLICT(position_id, fill_id) DO NOTHING
        "#,
    )
    .bind(position_id)
    .bind(fill_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn link_fill_to_closed_position_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    position_id: i64,
    fill_id: i64,
) -> Result<(), ProjectorRepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO closed_position_fills(position_id, fill_id)
        VALUES($1,$2)
        ON CONFLICT(position_id, fill_id) DO NOTHING
        "#,
    )
    .bind(position_id)
    .bind(fill_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn next_projector_position_id_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<i64, ProjectorRepositoryError> {
    let position_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT nextval('projector_position_id_seq')::BIGINT
        "#,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(position_id)
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

async fn load_trade_contexts_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: &TradeExecuted,
) -> Result<(StoredOrderContext, StoredOrderContext), ProjectorRepositoryError> {
    let maker = maybe_load_context_for_order_in_tx(
        tx,
        event.maker_reservation_id.as_deref(),
        event.maker_order_id,
    )
    .await?;
    let taker = maybe_load_context_for_order_in_tx(
        tx,
        event.taker_reservation_id.as_deref(),
        event.taker_order_id,
    )
    .await?;

    if !is_synthetic_liquidation_or_adl_trade(event) {
        return Ok((
            maker.ok_or_else(|| {
                missing_order_context_for_participant(event, TradeParticipant::Maker)
            })?,
            taker.ok_or_else(|| {
                missing_order_context_for_participant(event, TradeParticipant::Taker)
            })?,
        ));
    }

    let needs_synthetic = maker.is_none() || taker.is_none();
    let market_name = if needs_synthetic {
        Some(load_market_name_in_tx(tx, event.market_id).await?)
    } else {
        None
    };
    let maker_side = maker.as_ref().map(|context| context.side);
    let taker_side = taker.as_ref().map(|context| context.side);

    let maker = match maker {
        Some(context) => context,
        None => synthetic_liquidation_context(
            event,
            TradeParticipant::Maker,
            market_name.as_deref().unwrap_or_default(),
            taker_side,
        )?,
    };
    let taker = match taker {
        Some(context) => context,
        None => synthetic_liquidation_context(
            event,
            TradeParticipant::Taker,
            market_name.as_deref().unwrap_or_default(),
            maker_side,
        )?,
    };

    Ok((maker, taker))
}

async fn maybe_load_context_for_order_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reservation_id: Option<&str>,
    order_id: i64,
) -> Result<Option<StoredOrderContext>, ProjectorRepositoryError> {
    match load_context_for_order_in_tx(tx, reservation_id, order_id).await {
        Ok(context) => Ok(Some(context)),
        Err(error) if error.is_missing_order_context() => Ok(None),
        Err(error) => Err(error),
    }
}

async fn load_market_name_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    market_id: i64,
) -> Result<String, ProjectorRepositoryError> {
    let market_name = sqlx::query_scalar::<_, String>(
        r#"
        SELECT market_name
        FROM markets
        WHERE market_id=$1
        "#,
    )
    .bind(market_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(market_name)
}

fn synthetic_liquidation_context(
    event: &TradeExecuted,
    participant: TradeParticipant,
    market_name: &str,
    counterpart_side: Option<SideType>,
) -> Result<StoredOrderContext, ProjectorRepositoryError> {
    let side = synthetic_liquidation_side(event, participant, counterpart_side)
        .ok_or_else(|| missing_order_context_for_participant(event, participant))?;

    Ok(StoredOrderContext {
        reservation_id: participant
            .reservation_id(event)
            .map(String::from)
            .or_else(|| {
                event
                    .liquidation_id
                    .as_ref()
                    .map(|liquidation_id| format!("{liquidation_id}:{}", participant.label()))
            }),
        request_id: event.source_input_id.clone().unwrap_or_else(|| {
            format!(
                "synthetic:{}:{}:{}",
                event.market_id,
                event.engine_sequence,
                participant.label()
            )
        }),
        order_id: Some(participant.order_id(event)),
        user_id: participant.user_id(event),
        market_id: event.market_id,
        market_name: String::from(market_name),
        side,
        order_type: DbOrderType::MARKET,
        quantity: event.quantity,
        price: event.price,
        margin: 0,
        reduce_only: true,
    })
}

fn synthetic_liquidation_side(
    event: &TradeExecuted,
    participant: TradeParticipant,
    counterpart_side: Option<SideType>,
) -> Option<SideType> {
    if let Some(position_side) = event.position_side {
        if let Some(liquidated_user_id) = event.liquidated_user_id {
            return Some(if participant.user_id(event) == liquidated_user_id {
                opposite_side(position_side)
            } else {
                side_to_db(position_side)
            });
        }

        return Some(match participant {
            TradeParticipant::Maker => side_to_db(position_side),
            TradeParticipant::Taker => opposite_side(position_side),
        });
    }

    counterpart_side.map(opposite_db_side).or_else(|| {
        Some(match participant {
            TradeParticipant::Maker => SideType::LONG,
            TradeParticipant::Taker => SideType::SHORT,
        })
    })
}

fn is_synthetic_liquidation_or_adl_trade(event: &TradeExecuted) -> bool {
    event.execution_reason == ExecutionReason::LIQUIDATION
        || event.liquidation_id.is_some()
        || event.liquidated_user_id.is_some()
        || event.position_side.is_some()
}

fn missing_order_context_for_participant(
    event: &TradeExecuted,
    participant: TradeParticipant,
) -> ProjectorRepositoryError {
    ProjectorRepositoryError::MissingOrderContext {
        reservation_id: participant.reservation_id(event).map(String::from),
        order_id: Some(participant.order_id(event)),
    }
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
            COALESCE(w.amount, 0) AS margin,
            c.reduce_only
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
            COALESCE(w.amount, 0) AS margin,
            c.reduce_only
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
            margin,
            reduce_only
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
        reduce_only: row.get("reduce_only"),
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
            margin,
            reduce_only
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
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
    .bind(context.reduce_only)
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
            margin,
            reduce_only
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT(order_id)
        DO UPDATE
        SET status=CASE
                WHEN orders.status IN ('FILLED','CANCELLED') THEN orders.status
                ELSE EXCLUDED.status
            END,
            reduce_only=EXCLUDED.reduce_only,
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
    .bind(context.reduce_only)
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
            margin,
            reduce_only
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT(order_id)
        DO UPDATE
        SET status=EXCLUDED.status,
            reduce_only=EXCLUDED.reduce_only,
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
    .bind(context.reduce_only)
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

fn opposite_side(side: Side) -> SideType {
    match side {
        Side::LONG => SideType::SHORT,
        Side::SHORT => SideType::LONG,
    }
}

fn opposite_db_side(side: SideType) -> SideType {
    match side {
        SideType::LONG => SideType::SHORT,
        SideType::SHORT => SideType::LONG,
    }
}

fn order_type_to_db(order_type: OrderType) -> DbOrderType {
    match order_type {
        OrderType::LIMIT => DbOrderType::LIMIT,
        OrderType::MARKET => DbOrderType::MARKET,
    }
}

fn close_type_from_execution_reason(execution_reason: ExecutionReason) -> CloseType {
    match execution_reason {
        ExecutionReason::TRADE => CloseType::TRADE,
        ExecutionReason::LIQUIDATION => CloseType::LIQUIDATION,
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

fn proportional_margin(source_margin: i64, source_quantity: i64, target_quantity: i64) -> i64 {
    if source_margin <= 0 || source_quantity <= 0 || target_quantity <= 0 {
        return 0;
    }
    if target_quantity >= source_quantity {
        return source_margin;
    }

    let margin = (source_margin as i128 * target_quantity as i128) / source_quantity as i128;
    clamp_i128_to_i64(margin).clamp(1, source_margin)
}

fn weighted_average_price(
    current_average: i64,
    current_quantity: i64,
    fill_price: i64,
    fill_quantity: i64,
) -> i64 {
    let next_quantity = current_quantity + fill_quantity;
    if next_quantity <= 0 {
        return fill_price;
    }

    let notional = current_average as i128 * current_quantity as i128
        + fill_price as i128 * fill_quantity as i128;
    clamp_i128_to_i64(notional / next_quantity as i128)
}

fn realized_pnl(side: SideType, entry_price: i64, exit_price: i64, quantity: i64) -> i64 {
    let price_delta = match side {
        SideType::LONG => exit_price as i128 - entry_price as i128,
        SideType::SHORT => entry_price as i128 - exit_price as i128,
    };

    clamp_i128_to_i64(price_delta.saturating_mul(quantity as i128))
}

fn unrealized_pnl(side: SideType, average_price: i64, mark_price: i64, quantity: i64) -> i64 {
    if quantity <= 0 {
        return 0;
    }

    let price_delta = match side {
        SideType::LONG => mark_price as i128 - average_price as i128,
        SideType::SHORT => average_price as i128 - mark_price as i128,
    };

    clamp_i128_to_i64(price_delta.saturating_mul(quantity as i128))
}

fn clamp_i128_to_i64(value: i128) -> i64 {
    value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_offset_upsert_only_advances_offset() {
        let sql = SAVE_QUEUE_OFFSET_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(sql.contains("ON CONFLICT(topic, partition) DO UPDATE"));
        assert!(sql.contains("WHERE projector_offsets.next_offset < EXCLUDED.next_offset"));
    }

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
    fn synthetic_liquidation_context_closes_liquidated_taker_side() {
        let event = liquidation_trade_event();

        let context = synthetic_liquidation_context(
            &event,
            TradeParticipant::Taker,
            "SOL-PERP",
            Some(SideType::LONG),
        )
        .expect("liquidation context should be derivable");

        assert_eq!(context.reservation_id, Some(String::from("liq-1")));
        assert_eq!(context.order_id, Some(9003));
        assert_eq!(context.user_id, 42);
        assert_eq!(context.market_id, 7);
        assert_eq!(context.market_name, "SOL-PERP");
        assert_eq!(context.side, SideType::SHORT);
        assert_eq!(context.order_type, DbOrderType::MARKET);
        assert_eq!(context.quantity, 5);
        assert_eq!(context.price, 95);
        assert_eq!(context.margin, 0);
        assert!(context.reduce_only);
    }

    #[test]
    fn synthetic_liquidation_context_uses_position_side_for_counterparty() {
        let event = TradeExecuted {
            maker_reservation_id: None,
            ..liquidation_trade_event()
        };

        let context = synthetic_liquidation_context(
            &event,
            TradeParticipant::Maker,
            "SOL-PERP",
            Some(SideType::SHORT),
        )
        .expect("counterparty context should be derivable");

        assert_eq!(context.reservation_id, Some(String::from("liq-1:maker")));
        assert_eq!(context.order_id, Some(9001));
        assert_eq!(context.user_id, 43);
        assert_eq!(context.side, SideType::LONG);
        assert!(context.reduce_only);
    }

    #[test]
    fn synthetic_liquidation_side_can_fall_back_to_counterpart_side() {
        let event = TradeExecuted {
            position_side: None,
            liquidated_user_id: None,
            ..liquidation_trade_event()
        };

        assert_eq!(
            synthetic_liquidation_side(&event, TradeParticipant::Maker, Some(SideType::SHORT)),
            Some(SideType::LONG)
        );
    }

    #[test]
    fn synthetic_liquidation_side_uses_stable_default_without_side_hints() {
        let event = TradeExecuted {
            position_side: None,
            liquidated_user_id: None,
            ..liquidation_trade_event()
        };

        assert_eq!(
            synthetic_liquidation_side(&event, TradeParticipant::Maker, None),
            Some(SideType::LONG)
        );
        assert_eq!(
            synthetic_liquidation_side(&event, TradeParticipant::Taker, None),
            Some(SideType::SHORT)
        );
    }

    #[test]
    fn synthetic_context_fallback_is_not_enabled_for_plain_trades() {
        let plain_trade = TradeExecuted {
            execution_reason: ExecutionReason::TRADE,
            liquidation_id: None,
            liquidated_user_id: None,
            position_side: None,
            ..liquidation_trade_event()
        };
        let adl_like_trade = TradeExecuted {
            execution_reason: ExecutionReason::TRADE,
            liquidation_id: Some(String::from("liq-adl-1")),
            liquidated_user_id: Some(42),
            position_side: Some(Side::LONG),
            ..liquidation_trade_event()
        };

        assert!(!is_synthetic_liquidation_or_adl_trade(&plain_trade));
        assert!(is_synthetic_liquidation_or_adl_trade(&adl_like_trade));
    }

    #[test]
    fn protocol_order_type_mapping_matches_db_names() {
        assert_eq!(order_type_to_db(OrderType::LIMIT), DbOrderType::LIMIT);
        assert_eq!(order_type_to_db(OrderType::MARKET), DbOrderType::MARKET);
    }

    #[test]
    fn execution_reason_maps_to_closed_position_reason() {
        assert_eq!(
            close_type_from_execution_reason(ExecutionReason::TRADE),
            CloseType::TRADE
        );
        assert_eq!(
            close_type_from_execution_reason(ExecutionReason::LIQUIDATION),
            CloseType::LIQUIDATION
        );
    }

    #[test]
    fn proportional_margin_slices_source_margin() {
        assert_eq!(proportional_margin(1000, 10, 4), 400);
        assert_eq!(proportional_margin(1000, 10, 10), 1000);
        assert_eq!(proportional_margin(0, 10, 4), 0);
    }

    #[test]
    fn weighted_average_price_uses_position_notional() {
        assert_eq!(weighted_average_price(100, 10, 120, 10), 110);
        assert_eq!(weighted_average_price(100, 3, 101, 1), 100);
    }

    #[test]
    fn realized_pnl_uses_position_side() {
        assert_eq!(realized_pnl(SideType::LONG, 100, 120, 10), 200);
        assert_eq!(realized_pnl(SideType::LONG, 100, 90, 10), -100);
        assert_eq!(realized_pnl(SideType::SHORT, 100, 80, 10), 200);
        assert_eq!(realized_pnl(SideType::SHORT, 100, 110, 10), -100);
    }

    #[test]
    fn unrealized_pnl_uses_mark_price_and_position_side() {
        assert_eq!(unrealized_pnl(SideType::LONG, 100, 120, 10), 200);
        assert_eq!(unrealized_pnl(SideType::LONG, 100, 90, 10), -100);
        assert_eq!(unrealized_pnl(SideType::SHORT, 100, 80, 10), 200);
        assert_eq!(unrealized_pnl(SideType::SHORT, 100, 110, 10), -100);
    }

    #[test]
    fn unrealized_pnl_clamps_large_values() {
        assert_eq!(
            unrealized_pnl(SideType::LONG, 0, i64::MAX, i64::MAX),
            i64::MAX
        );
    }

    #[test]
    fn engine_event_log_entry_uses_engine_event_id() {
        let event = EngineEvent::RiskStateUpdated(protocol::engine::RiskStateUpdated {
            engine_event_id: Some(String::from("eng-1")),
            market_id: 7,
            engine_sequence: 42,
            engine_timestamp_ms: 1_710_000_000_000,
            source_input_id: Some(String::from("input-1")),
            source_input_offset: None,
            user_id: 99,
            position_id: String::from("pos-1"),
            mark_price: 100,
            equity: 1_000,
            maintenance_margin: 100,
            margin_ratio: 10,
            status: String::from("HEALTHY"),
        });

        let entry = engine_event_log_entry(&event).expect("event should serialize");

        assert_eq!(entry.engine_event_id, "eng-1");
        assert_eq!(entry.event_type, "RiskStateUpdated");
        assert_eq!(entry.market_id, Some(7));
        assert_eq!(entry.engine_sequence, Some(42));
        assert_eq!(entry.engine_timestamp_ms, 1_710_000_000_000);
        assert_eq!(entry.payload["type"], "RiskStateUpdated");
        assert_eq!(entry.payload["payload"]["engine_event_id"], "eng-1");
    }

    #[test]
    fn engine_event_log_entry_falls_back_for_legacy_event_without_id() {
        let event = EngineEvent::FundingRateUpdated(protocol::engine::FundingRateUpdated {
            engine_event_id: None,
            market_id: 7,
            engine_sequence: 43,
            engine_timestamp_ms: 1_710_000_000_001,
            source_input_id: None,
            source_input_offset: Some(123),
            funding_interval_id: String::from("funding-1"),
            rate: 12,
            rate_scale: 1_000_000,
            interval_start_ms: 1_710_000_000_000,
            interval_end_ms: 1_710_003_600_000,
        });

        let entry = engine_event_log_entry(&event).expect("event should serialize");

        assert_eq!(entry.engine_event_id, "FundingRateUpdated:7:43");
        assert_eq!(entry.event_type, "FundingRateUpdated");
        assert_eq!(entry.market_id, Some(7));
        assert_eq!(entry.engine_sequence, Some(43));
    }

    fn liquidation_trade_event() -> TradeExecuted {
        TradeExecuted {
            engine_event_id: Some(String::from("eng-7-11")),
            engine_sequence: 11,
            engine_timestamp_ms: 1_710_000_000_000,
            source_input_id: Some(String::from("mark-1")),
            source_input_offset: Some(10),
            fill_id: 7002,
            market_id: 7,
            price: 95,
            quantity: 5,
            maker_order_id: 9001,
            taker_order_id: 9003,
            maker_user_id: 43,
            taker_user_id: 42,
            maker_reservation_id: Some(String::from("res-maker")),
            taker_reservation_id: Some(String::from("liq-1")),
            execution_reason: ExecutionReason::LIQUIDATION,
            liquidation_id: Some(String::from("liq-1")),
            liquidated_user_id: Some(42),
            position_side: Some(Side::LONG),
            liquidation_fee: None,
            fee_deltas: Vec::new(),
            settlements: Vec::new(),
        }
    }
}
