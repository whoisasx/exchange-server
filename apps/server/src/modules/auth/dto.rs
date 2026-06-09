use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct UserRecord{
  pub username: String,
  pub userid: u64,
  pub jwt_token: String
}

#[derive(Deserialize)]
pub struct AuthUser{
  pub username: String,
  pub password: String
}

#[derive(Debug,Serialize,Deserialize)]
pub struct Claim{
  pub userid: u64,
  pub username: String
}