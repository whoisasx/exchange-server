use chrono::{DateTime, Utc};
use db::dto::UserRow;
use serde::Serialize;

#[derive(Serialize)]
pub struct UserProfile {
    pub user_id: i64,
    pub username: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl From<UserRow> for UserProfile {
    fn from(user: UserRow) -> Self {
        Self {
            user_id: user.user_id,
            username: user.username,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}
