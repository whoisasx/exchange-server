use protocol::{engine::EngineEvent, wallet::WalletEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ClientMessage {
    Subscribe(MarketSubscription),
    Unsubscribe(MarketSubscription),
    Ping(PingPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MarketSubscription {
    pub markets: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PingPayload {
    pub nonce: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerMessage {
    Welcome(WelcomePayload),
    AccountEvent(EventPayload),
    MarketEvent(MarketEventPayload),
    Subscribed(MarketSubscription),
    Unsubscribed(MarketSubscription),
    Pong(PingPayload),
    Error(ErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WelcomePayload {
    pub user_id: i64,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EventPayload {
    pub source: EventSource,
    pub event: Value,
    pub metadata: StreamMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MarketEventPayload {
    pub market_id: i64,
    pub source: EventSource,
    pub event: Value,
    pub metadata: StreamMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventSource {
    Engine,
    Wallet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorPayload {
    pub message: String,
}

impl ServerMessage {
    pub fn account_event(
        source: EventSource,
        event: Value,
        metadata: StreamMetadata,
    ) -> ServerMessage {
        ServerMessage::AccountEvent(EventPayload {
            source,
            event,
            metadata,
        })
    }

    pub fn market_event(
        market_id: i64,
        source: EventSource,
        event: Value,
        metadata: StreamMetadata,
    ) -> ServerMessage {
        ServerMessage::MarketEvent(MarketEventPayload {
            market_id,
            source,
            event,
            metadata,
        })
    }

    pub fn error(message: impl Into<String>) -> ServerMessage {
        ServerMessage::Error(ErrorPayload {
            message: message.into(),
        })
    }
}

pub fn engine_event_value(event: &EngineEvent) -> Result<Value, serde_json::Error> {
    serde_json::to_value(event)
}

pub fn wallet_event_value(event: &WalletEvent) -> Result<Value, serde_json::Error> {
    serde_json::to_value(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_subscribe_message() {
        let message = serde_json::from_str::<ClientMessage>(
            r#"{"type":"Subscribe","payload":{"markets":[1,2]}}"#,
        )
        .expect("subscribe message should parse");

        assert_eq!(
            message,
            ClientMessage::Subscribe(MarketSubscription {
                markets: vec![1, 2]
            })
        );
    }

    #[test]
    fn serializes_pong_message() {
        let value = serde_json::to_value(ServerMessage::Pong(PingPayload {
            nonce: Some(String::from("n1")),
        }))
        .expect("pong should serialize");

        assert_eq!(value["type"], "Pong");
        assert_eq!(value["payload"]["nonce"], "n1");
    }
}
