use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Handler, StreamHandler, WrapFuture,
};
use actix_web_actors::ws;

use crate::{
    auth::Claim,
    hub::{Hub, ServerPush},
    messages::{ClientMessage, MarketSubscription, ServerMessage, WelcomePayload},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(90);
const MAILBOX_CAPACITY: usize = 1024;

pub struct WsSession {
    session_id: Option<u64>,
    claim: Claim,
    hub: Hub,
    heartbeat_at: Instant,
}

impl WsSession {
    pub fn new(claim: Claim, hub: Hub) -> Self {
        Self {
            session_id: None,
            claim,
            hub,
            heartbeat_at: Instant::now(),
        }
    }

    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |actor, ctx| {
            if Instant::now().duration_since(actor.heartbeat_at) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }

            ctx.ping(b"");
        });
    }

    fn handle_text(&mut self, text: &str, ctx: &mut ws::WebsocketContext<Self>) {
        match serde_json::from_str::<ClientMessage>(text) {
            Ok(ClientMessage::Subscribe(payload)) => self.subscribe(payload.markets, ctx),
            Ok(ClientMessage::Unsubscribe(payload)) => self.unsubscribe(payload.markets, ctx),
            Ok(ClientMessage::Ping(payload)) => {
                send_server_message(ctx, ServerMessage::Pong(payload))
            }
            Err(_) => send_server_message(ctx, ServerMessage::error("invalid client message")),
        }
    }

    fn subscribe(&self, markets: Vec<i64>, ctx: &mut ws::WebsocketContext<Self>) {
        let Some(session_id) = self.session_id else {
            send_server_message(ctx, ServerMessage::error("session is not ready"));
            return;
        };
        let markets = match normalize_markets(markets) {
            Ok(markets) => markets,
            Err(message) => {
                send_server_message(ctx, ServerMessage::error(message));
                return;
            }
        };

        let hub = self.hub.clone();
        let response_markets = markets.clone();
        ctx.spawn(
            async move {
                hub.subscribe_markets(session_id, &markets).await;
            }
            .into_actor(self)
            .map(move |(), _actor, ctx| {
                send_server_message(
                    ctx,
                    ServerMessage::Subscribed(MarketSubscription {
                        markets: response_markets,
                    }),
                );
            }),
        );
    }

    fn unsubscribe(&self, markets: Vec<i64>, ctx: &mut ws::WebsocketContext<Self>) {
        let Some(session_id) = self.session_id else {
            send_server_message(ctx, ServerMessage::error("session is not ready"));
            return;
        };
        let markets = match normalize_markets(markets) {
            Ok(markets) => markets,
            Err(message) => {
                send_server_message(ctx, ServerMessage::error(message));
                return;
            }
        };

        let hub = self.hub.clone();
        let response_markets = markets.clone();
        ctx.spawn(
            async move {
                hub.unsubscribe_markets(session_id, &markets).await;
            }
            .into_actor(self)
            .map(move |(), _actor, ctx| {
                send_server_message(
                    ctx,
                    ServerMessage::Unsubscribed(MarketSubscription {
                        markets: response_markets,
                    }),
                );
            }),
        );
    }
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(MAILBOX_CAPACITY);
        self.start_heartbeat(ctx);

        let recipient = ctx.address().recipient();
        let hub = self.hub.clone();
        let user_id = self.claim.userid;
        let welcome = ServerMessage::Welcome(WelcomePayload {
            user_id,
            username: self.claim.username.clone(),
        });

        ctx.spawn(
            async move { hub.register(user_id, recipient).await }
                .into_actor(self)
                .map(move |session_id, actor, ctx| {
                    actor.session_id = Some(session_id);
                    send_server_message(ctx, welcome.clone());
                }),
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let Some(session_id) = self.session_id else {
            return;
        };
        let hub = self.hub.clone();

        actix::spawn(async move {
            hub.unregister(session_id).await;
        });
    }
}

impl Handler<ServerPush> for WsSession {
    type Result = ();

    fn handle(&mut self, message: ServerPush, ctx: &mut Self::Context) {
        ctx.text(message.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(bytes)) => {
                self.heartbeat_at = Instant::now();
                ctx.pong(&bytes);
            }
            Ok(ws::Message::Pong(_)) => {
                self.heartbeat_at = Instant::now();
            }
            Ok(ws::Message::Text(text)) => self.handle_text(&text, ctx),
            Ok(ws::Message::Binary(_)) => {
                send_server_message(
                    ctx,
                    ServerMessage::error("binary messages are not supported"),
                );
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Continuation(_)) => {
                send_server_message(
                    ctx,
                    ServerMessage::error("continuation messages are not supported"),
                );
            }
            Ok(ws::Message::Nop) => {}
            Err(_) => ctx.stop(),
        }
    }
}

fn send_server_message(ctx: &mut ws::WebsocketContext<WsSession>, message: ServerMessage) {
    match serde_json::to_string(&message) {
        Ok(text) => ctx.text(text),
        Err(_) => ctx
            .text(r#"{"type":"Error","payload":{"message":"failed to serialize server message"}}"#),
    }
}

fn normalize_markets(markets: Vec<i64>) -> Result<Vec<i64>, String> {
    if markets.iter().any(|market_id| *market_id <= 0) {
        return Err(String::from("market ids must be greater than zero"));
    }

    Ok(markets
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::messages::PingPayload;

    use super::*;

    #[test]
    fn normalize_markets_deduplicates_and_sorts() {
        assert_eq!(normalize_markets(vec![3, 1, 3]).unwrap(), vec![1, 3]);
    }

    #[test]
    fn normalize_markets_rejects_non_positive_ids() {
        assert!(normalize_markets(vec![1, 0]).is_err());
    }

    #[test]
    fn ping_payload_is_supported() {
        let payload =
            serde_json::from_str::<ClientMessage>(r#"{"type":"Ping","payload":{"nonce":"abc"}}"#)
                .expect("ping should parse");

        assert_eq!(
            payload,
            ClientMessage::Ping(PingPayload {
                nonce: Some(String::from("abc"))
            })
        );
    }
}
