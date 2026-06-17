use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use actix::{Message, Recipient};
use serde_json::Error as SerializeError;
use tokio::sync::Mutex;

use crate::messages::ServerMessage;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ServerPush(pub String);

#[derive(Clone)]
pub struct Hub {
    inner: Arc<Mutex<HubState>>,
    next_session_id: Arc<AtomicU64>,
}

impl Default for Hub {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HubState::default())),
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl Hub {
    pub async fn register(&self, user_id: i64, recipient: Recipient<ServerPush>) -> u64 {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let mut state = self.inner.lock().await;

        state.sessions.insert(
            session_id,
            SessionHandle {
                user_id,
                recipient,
                markets: HashSet::new(),
            },
        );
        state.users.entry(user_id).or_default().insert(session_id);

        session_id
    }

    pub async fn unregister(&self, session_id: u64) {
        let mut state = self.inner.lock().await;
        remove_session_locked(&mut state, session_id);
    }

    pub async fn subscribe_markets(&self, session_id: u64, markets: &[i64]) {
        let mut state = self.inner.lock().await;

        for market_id in markets {
            if let Some(session) = state.sessions.get_mut(&session_id) {
                session.markets.insert(*market_id);
            }
            state
                .markets
                .entry(*market_id)
                .or_default()
                .insert(session_id);
        }
    }

    pub async fn unsubscribe_markets(&self, session_id: u64, markets: &[i64]) {
        let mut state = self.inner.lock().await;

        for market_id in markets {
            if let Some(session) = state.sessions.get_mut(&session_id) {
                session.markets.remove(market_id);
            }
            if let Some(sessions) = state.markets.get_mut(market_id) {
                sessions.remove(&session_id);
                if sessions.is_empty() {
                    state.markets.remove(market_id);
                }
            }
        }
    }

    pub async fn broadcast_account(
        &self,
        user_id: i64,
        message: &ServerMessage,
    ) -> Result<usize, SerializeError> {
        let text = serde_json::to_string(message)?;
        let mut state = self.inner.lock().await;
        let sessions = state.users.get(&user_id).cloned().unwrap_or_default();

        Ok(send_to_sessions_locked(&mut state, sessions, &text))
    }

    pub async fn broadcast_market(
        &self,
        market_id: i64,
        message: &ServerMessage,
    ) -> Result<usize, SerializeError> {
        let text = serde_json::to_string(message)?;
        let mut state = self.inner.lock().await;
        let sessions = state.markets.get(&market_id).cloned().unwrap_or_default();

        Ok(send_to_sessions_locked(&mut state, sessions, &text))
    }
}

#[derive(Default)]
struct HubState {
    sessions: HashMap<u64, SessionHandle>,
    users: HashMap<i64, HashSet<u64>>,
    markets: HashMap<i64, HashSet<u64>>,
}

struct SessionHandle {
    user_id: i64,
    recipient: Recipient<ServerPush>,
    markets: HashSet<i64>,
}

fn send_to_sessions_locked(state: &mut HubState, sessions: HashSet<u64>, text: &str) -> usize {
    let mut stale = Vec::new();
    let mut delivered = 0;

    for session_id in sessions {
        let Some(session) = state.sessions.get(&session_id) else {
            continue;
        };

        match session.recipient.try_send(ServerPush(String::from(text))) {
            Ok(()) => delivered += 1,
            Err(_) => stale.push(session_id),
        }
    }

    for session_id in stale {
        remove_session_locked(state, session_id);
    }

    delivered
}

fn remove_session_locked(state: &mut HubState, session_id: u64) {
    let Some(session) = state.sessions.remove(&session_id) else {
        return;
    };

    if let Some(sessions) = state.users.get_mut(&session.user_id) {
        sessions.remove(&session_id);
        if sessions.is_empty() {
            state.users.remove(&session.user_id);
        }
    }

    for market_id in session.markets {
        if let Some(sessions) = state.markets.get_mut(&market_id) {
            sessions.remove(&session_id);
            if sessions.is_empty() {
                state.markets.remove(&market_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};
    use std::time::Duration;

    use actix::{Actor, Context, Handler};

    use crate::messages::{ErrorPayload, ServerMessage};

    use super::*;

    struct Collector {
        messages: StdArc<StdMutex<Vec<String>>>,
    }

    impl Actor for Collector {
        type Context = Context<Self>;
    }

    impl Handler<ServerPush> for Collector {
        type Result = ();

        fn handle(&mut self, message: ServerPush, _ctx: &mut Self::Context) {
            self.messages.lock().unwrap().push(message.0);
        }
    }

    #[actix::test]
    async fn broadcast_account_only_reaches_matching_user() {
        let hub = Hub::default();
        let user_messages = StdArc::new(StdMutex::new(Vec::new()));
        let other_messages = StdArc::new(StdMutex::new(Vec::new()));
        let user = Collector {
            messages: user_messages.clone(),
        }
        .start();
        let other = Collector {
            messages: other_messages.clone(),
        }
        .start();

        hub.register(42, user.recipient()).await;
        hub.register(7, other.recipient()).await;
        let delivered = hub
            .broadcast_account(
                42,
                &ServerMessage::Error(ErrorPayload {
                    message: String::from("account"),
                }),
            )
            .await
            .expect("account message should serialize");

        actix::clock::sleep(Duration::from_millis(10)).await;

        assert_eq!(delivered, 1);
        assert_eq!(user_messages.lock().unwrap().len(), 1);
        assert!(other_messages.lock().unwrap().is_empty());
    }

    #[actix::test]
    async fn broadcast_market_reaches_subscribed_session() {
        let hub = Hub::default();
        let messages = StdArc::new(StdMutex::new(Vec::new()));
        let collector = Collector {
            messages: messages.clone(),
        }
        .start();
        let session_id = hub.register(42, collector.recipient()).await;
        hub.subscribe_markets(session_id, &[9]).await;

        let delivered = hub
            .broadcast_market(
                9,
                &ServerMessage::Error(ErrorPayload {
                    message: String::from("market"),
                }),
            )
            .await
            .expect("market message should serialize");

        actix::clock::sleep(Duration::from_millis(10)).await;

        assert_eq!(delivered, 1);
        assert_eq!(messages.lock().unwrap().len(), 1);
    }
}
