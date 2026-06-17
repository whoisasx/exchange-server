use std::fmt;

use actix_web::{HttpRequest, http::header};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Claim {
    pub userid: i64,
    pub username: String,
    pub exp: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    InvalidToken,
    MissingToken,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken => f.write_str("invalid token"),
            Self::MissingToken => f.write_str("missing token"),
        }
    }
}

pub fn authenticate_request(req: &HttpRequest, jwt_secret: &str) -> Result<Claim, AuthError> {
    let token = token_from_authorization(req)
        .or_else(|| token_from_query(req.query_string()))
        .ok_or(AuthError::MissingToken)?;

    decode::<Claim>(
        &token,
        &DecodingKey::from_secret(jwt_secret.as_ref()),
        &Validation::new(Algorithm::HS256),
    )
    .map(|payload| payload.claims)
    .map_err(|_| AuthError::InvalidToken)
}

fn token_from_authorization(req: &HttpRequest) -> Option<String> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(String::from)
}

fn token_from_query(query: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "token" && !value.trim().is_empty()).then(|| String::from(value.trim()))
    })
}

#[cfg(test)]
mod tests {
    use actix_web::{http::header, test as actix_test};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct TestClaim {
        userid: i64,
        username: String,
        exp: usize,
    }

    fn token(secret: &str) -> String {
        encode(
            &Header::default(),
            &TestClaim {
                userid: 42,
                username: String::from("alice"),
                exp: 4_102_444_800,
            },
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .expect("test token should encode")
    }

    #[test]
    fn authenticates_bearer_token() {
        let secret = "test-secret";
        let req = actix_test::TestRequest::get()
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", token(secret))))
            .to_http_request();

        let claim = authenticate_request(&req, secret).expect("token should authenticate");

        assert_eq!(claim.userid, 42);
        assert_eq!(claim.username, "alice");
    }

    #[test]
    fn authenticates_query_token() {
        let secret = "test-secret";
        let req = actix_test::TestRequest::get()
            .uri(&format!("/ws?token={}", token(secret)))
            .to_http_request();

        let claim = authenticate_request(&req, secret).expect("token should authenticate");

        assert_eq!(claim.userid, 42);
    }

    #[test]
    fn rejects_missing_token() {
        let req = actix_test::TestRequest::get().uri("/ws").to_http_request();

        assert_eq!(
            authenticate_request(&req, "test-secret").unwrap_err(),
            AuthError::MissingToken
        );
    }
}
