use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct UserRecord {
    pub username: String,
    pub userid: i64,
    pub jwt_token: String,
}

#[derive(Deserialize)]
pub struct AuthUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claim {
    pub userid: i64,
    pub username: String,
}
