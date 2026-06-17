use serde::{Deserialize, Serialize};

use crate::common::{Asset, CommandEnvelope, OrderType, Side};

pub const ENGINE_COMMANDS_TOPIC: &str = "engine.commands";
pub const ENGINE_REPLIES_TOPIC: &str = "engine.replies";
pub const ENGINE_EVENTS_TOPIC: &str = "engine.events";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineCommand {
    PlaceOrder(ReservedPlaceOrder),
    CancelOrder(CancelOrder),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservedPlaceOrder {
    pub envelope: CommandEnvelope,
    pub reservation_id: String,
    pub market_id: i64,
    pub market_name: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrder {
    pub envelope: CommandEnvelope,
    pub market_id: i64,
    pub order_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineReply {
    OrderAccepted(OrderAccepted),
    OrderRejected(OrderRejected),
    CancelAccepted(CancelAccepted),
    CancelRejected(CancelRejected),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAccepted {
    pub request_id: String,
    pub order_id: i64,
    pub reservation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRejected {
    pub request_id: String,
    pub reservation_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAccepted {
    pub request_id: String,
    pub order_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRejected {
    pub request_id: String,
    pub order_id: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineEvent {
    OrderOpened(OrderOpened),
    OrderCancelled(OrderCancelled),
    TradeExecuted(TradeExecuted),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderOpened {
    pub order_id: i64,
    pub reservation_id: String,
    pub user_id: i64,
    pub market_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub order_id: i64,
    pub reservation_id: String,
    pub released_amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeExecuted {
    pub fill_id: i64,
    pub market_id: i64,
    pub price: i64,
    pub quantity: i64,
    pub maker_order_id: i64,
    pub taker_order_id: i64,
    pub maker_reservation_id: Option<String>,
    pub taker_reservation_id: Option<String>,
    #[serde(default)]
    pub settlements: Vec<TradeSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeSettlement {
    pub reservation_id: String,
    pub debit_asset: Asset,
    pub debit_amount: i64,
    pub credit_asset: Asset,
    pub credit_amount: i64,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn trade_executed_defaults_missing_settlements() {
        let event = serde_json::from_str::<EngineEvent>(
            r#"{
                "type":"TradeExecuted",
                "payload":{
                    "fill_id":1,
                    "market_id":2,
                    "price":100,
                    "quantity":5,
                    "maker_order_id":10,
                    "taker_order_id":11,
                    "maker_reservation_id":"res-maker",
                    "taker_reservation_id":"res-taker"
                }
            }"#,
        )
        .expect("legacy trade event should deserialize");

        match event {
            EngineEvent::TradeExecuted(trade) => assert!(trade.settlements.is_empty()),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn command_fixtures_match_protocol_contract() {
        assert_command_fixture(include_str!(
            "../../../docs/streams/examples/engine-place-order.command.json"
        ));
        assert_command_fixture(include_str!(
            "../../../docs/streams/examples/engine-cancel-order.command.json"
        ));
    }

    #[test]
    fn reply_fixtures_match_protocol_contract() {
        assert_reply_fixture(include_str!(
            "../../../docs/streams/examples/engine-order-accepted.reply.json"
        ));
        assert_reply_fixture(include_str!(
            "../../../docs/streams/examples/engine-order-rejected.reply.json"
        ));
        assert_reply_fixture(include_str!(
            "../../../docs/streams/examples/engine-cancel-accepted.reply.json"
        ));
        assert_reply_fixture(include_str!(
            "../../../docs/streams/examples/engine-cancel-rejected.reply.json"
        ));
    }

    #[test]
    fn event_fixtures_match_protocol_contract() {
        assert_event_fixture(include_str!(
            "../../../docs/streams/examples/engine-order-opened.event.json"
        ));
        assert_event_fixture(include_str!(
            "../../../docs/streams/examples/engine-order-cancelled.event.json"
        ));
        assert_event_fixture(include_str!(
            "../../../docs/streams/examples/engine-trade-executed.event.json"
        ));
    }

    fn assert_command_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineCommand>(json)
            .expect("command fixture should match EngineCommand");
        assert_round_trip(json, &parsed);
    }

    fn assert_reply_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineReply>(json)
            .expect("reply fixture should match EngineReply");
        assert_round_trip(json, &parsed);
    }

    fn assert_event_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineEvent>(json)
            .expect("event fixture should match EngineEvent");
        assert_round_trip(json, &parsed);
    }

    fn assert_round_trip<T>(json: &str, parsed: &T)
    where
        T: serde::Serialize,
    {
        let fixture = serde_json::from_str::<Value>(json).expect("fixture should be valid JSON");
        let serialized = serde_json::to_value(parsed).expect("protocol value should serialize");

        assert_eq!(serialized, fixture);
    }
}
