use std::time::Duration;

use actix_web::{HttpRequest, HttpResponse, web};
use config::Config;
use protocol::common::CommandEnvelope;
use serde::Serialize;
use tokio::{sync::oneshot, time::timeout};
use uuid::Uuid;

use crate::{
    modules::auth::dto::Claim, redpanda::RedpandaProducer, replies::ReplyRecord,
    replies::ReplyState, utils::types::ResponseBody,
};

#[derive(Debug, Clone)]
pub struct HotPathCommandContext {
    pub envelope: CommandEnvelope,
    pub ack: QueuedCommandAck,
    pub wait_timeout: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueuedCommandAck {
    pub request_id: String,
    pub idempotency_key: String,
    pub status: String,
}

pub fn command_context(
    req: &HttpRequest,
    claim: &Claim,
) -> Result<HotPathCommandContext, HttpResponse> {
    let config = req
        .app_data::<web::Data<Config>>()
        .ok_or_else(|| internal_server_error("server config is not initialized"))?;
    let idempotency_key = idempotency_key(req)?;
    let request_id = format!("req_{}", Uuid::new_v4());

    Ok(HotPathCommandContext {
        envelope: CommandEnvelope {
            request_id: request_id.clone(),
            idempotency_key: idempotency_key.clone(),
            user_id: claim.userid,
            reply_partition: config.server_reply_partition,
        },
        ack: QueuedCommandAck {
            request_id,
            idempotency_key,
            status: String::from("queued"),
        },
        wait_timeout: Duration::from_millis(config.request_wait_timeout_ms),
    })
}

pub fn redpanda_producer(req: &HttpRequest) -> Result<web::Data<RedpandaProducer>, HttpResponse> {
    req.app_data::<web::Data<RedpandaProducer>>()
        .cloned()
        .ok_or_else(|| internal_server_error("redpanda producer is not initialized"))
}

pub fn reply_state(req: &HttpRequest) -> Result<web::Data<ReplyState>, HttpResponse> {
    req.app_data::<web::Data<ReplyState>>()
        .cloned()
        .ok_or_else(|| internal_server_error("reply state is not initialized"))
}

pub fn queued_response(ack: QueuedCommandAck) -> HttpResponse {
    HttpResponse::Accepted().json(ResponseBody {
        success: true,
        info: String::from("request queued"),
        body: Some(ack),
    })
}

pub async fn final_or_queued_response(
    receiver: oneshot::Receiver<ReplyRecord>,
    wait_timeout: Duration,
    ack: QueuedCommandAck,
) -> HttpResponse {
    match timeout(wait_timeout, receiver).await {
        Ok(Ok(record)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("request completed"),
            body: Some(record),
        }),
        Ok(Err(_)) | Err(_) => queued_response(ack),
    }
}

pub fn bad_request(info: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(ResponseBody::<()> {
        success: false,
        info: info.into(),
        body: None,
    })
}

pub fn internal_server_error(info: impl Into<String>) -> HttpResponse {
    HttpResponse::InternalServerError().json(ResponseBody::<()> {
        success: false,
        info: info.into(),
        body: None,
    })
}

fn idempotency_key(req: &HttpRequest) -> Result<String, HttpResponse> {
    let value = req
        .headers()
        .get("Idempotency-Key")
        .ok_or_else(|| bad_request("Idempotency-Key header is required"))?;
    let key = value
        .to_str()
        .map_err(|_| bad_request("Idempotency-Key header must be valid UTF-8"))?
        .trim();

    if key.is_empty() {
        return Err(bad_request("Idempotency-Key header cannot be empty"));
    }

    Ok(String::from(key))
}

#[cfg(test)]
mod tests {
    use actix_web::test as actix_test;

    use super::*;

    fn test_claim() -> Claim {
        Claim {
            userid: 42,
            username: String::from("alice"),
            exp: 1,
        }
    }

    fn test_config() -> Config {
        Config {
            database_url: String::from("postgres://localhost/test"),
            timeseries_database_url: None,
            server_url: String::from("http://localhost:8080"),
            server_port: 8080,
            server_host: String::from("127.0.0.1"),
            jwt_secret: String::from("test-secret"),
            redpanda_brokers: String::from("localhost:9092"),
            wallet_commands_topic: String::from("wallet.commands"),
            wallet_replies_topic: String::from("wallet.replies"),
            engine_replies_topic: String::from("engine.replies"),
            server_reply_partition: 2,
            request_wait_timeout_ms: 5000,
        }
    }

    #[test]
    fn command_context_uses_idempotency_header_and_reply_partition() {
        let req = actix_test::TestRequest::post()
            .insert_header(("Idempotency-Key", "client-key-1"))
            .app_data(web::Data::new(test_config()))
            .to_http_request();

        let context = command_context(&req, &test_claim()).expect("context should be built");

        assert_eq!(context.envelope.idempotency_key, "client-key-1");
        assert_eq!(context.envelope.user_id, 42);
        assert_eq!(context.envelope.reply_partition, 2);
        assert_eq!(context.ack.status, "queued");
    }

    #[test]
    fn command_context_requires_idempotency_header() {
        let req = actix_test::TestRequest::post()
            .app_data(web::Data::new(test_config()))
            .to_http_request();

        let response = command_context(&req, &test_claim()).expect_err("header should be required");

        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
}
