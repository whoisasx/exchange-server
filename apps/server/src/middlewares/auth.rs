use actix_web::{
    Error, HttpMessage, HttpResponse,
    body::BoxBody,
    dev::{ServiceRequest, ServiceResponse},
    http::header,
    middleware::Next,
    web,
};
use config::Config;
use jsonwebtoken::{DecodingKey, Validation, decode};

use crate::{modules::auth::dto::Claim, utils::types::ResponseBody};

pub async fn auth_middleware(
    config: web::Data<Config>,
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let auth_header = match req.headers().get(header::AUTHORIZATION) {
        Some(auth) => match auth.to_str() {
            Ok(v) => v,
            Err(_) => {
                let response = HttpResponse::Unauthorized().json(ResponseBody::<()> {
                    success: false,
                    info: String::from("invalid header encoding"),
                    body: None,
                });
                return Ok(req.into_response(response));
            }
        },
        None => {
            let response = HttpResponse::Unauthorized().json(ResponseBody::<()> {
                success: false,
                info: String::from("authorization header is missing"),
                body: None,
            });
            return Ok(req.into_response(response));
        }
    };

    if !auth_header.starts_with("Bearer ") {
        let response = HttpResponse::Unauthorized().json(ResponseBody::<()> {
            success: false,
            info: String::from("authorization header must contain 'Bearer ' "),
            body: None,
        });
        return Ok(req.into_response(response));
    }

    let auth_token = &auth_header[7..];

    let payload = match decode::<Claim>(
        &auth_token,
        &DecodingKey::from_secret(config.jwt_secret.as_ref()),
        &Validation::new(jsonwebtoken::Algorithm::HS256),
    ) {
        Ok(pl) => pl.claims,
        Err(_) => {
            let response = HttpResponse::Unauthorized().json(ResponseBody::<()> {
                success: false,
                info: String::from("invalid token"),
                body: None,
            });
            return Ok(req.into_response(response));
        }
    };

    req.extensions_mut().insert(payload);

    next.call(req).await
}

#[cfg(test)]
mod tests {
    use actix_web::{
        App, HttpMessage, HttpRequest, HttpResponse, body::to_bytes, http::header,
        middleware::from_fn, test, web,
    };
    use chrono::{Duration, Utc};
    use jsonwebtoken::{EncodingKey, Header, encode};

    use super::*;

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
            server_reply_partition: 0,
            request_wait_timeout_ms: 5000,
        }
    }

    async fn protected(req: HttpRequest) -> HttpResponse {
        let username = req
            .extensions()
            .get::<Claim>()
            .map(|claim| claim.username.clone())
            .unwrap_or_default();

        HttpResponse::Ok().body(username)
    }

    #[actix_web::test]
    async fn rejects_missing_authorization_header() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(test_config()))
                .wrap(from_fn(auth_middleware))
                .route("/protected", web::get().to(protected)),
        )
        .await;

        let req = test::TestRequest::get().uri("/protected").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn rejects_non_bearer_authorization_header() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(test_config()))
                .wrap(from_fn(auth_middleware))
                .route("/protected", web::get().to(protected)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/protected")
            .insert_header((header::AUTHORIZATION, "Token abc"))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn rejects_invalid_bearer_token() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(test_config()))
                .wrap(from_fn(auth_middleware))
                .route("/protected", web::get().to(protected)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/protected")
            .insert_header((header::AUTHORIZATION, "Bearer not-a-jwt"))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn accepts_valid_bearer_token_and_inserts_claim() {
        let config = test_config();
        let token = encode(
            &Header::default(),
            &Claim {
                userid: 42,
                username: String::from("alice"),
                exp: (Utc::now() + Duration::hours(1)).timestamp() as usize,
            },
            &EncodingKey::from_secret(config.jwt_secret.as_ref()),
        )
        .expect("test token should encode");

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config))
                .wrap(from_fn(auth_middleware))
                .route("/protected", web::get().to(protected)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/protected")
            .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = to_bytes(resp.into_body())
            .await
            .expect("response body should be readable");
        assert_eq!(body.as_ref(), b"alice");
    }
}
