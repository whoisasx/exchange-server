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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_profile_uses_public_user_fields() {
        let user = UserRow {
            user_id: 42,
            username: String::from("alice"),
            hashed_password: String::from("not-public"),
            created_at: None,
            updated_at: None,
        };

        let profile = UserProfile::from(user);

        assert_eq!(profile.user_id, 42);
        assert_eq!(profile.username, "alice");
        assert!(profile.created_at.is_none());
        assert!(profile.updated_at.is_none());
    }
}
