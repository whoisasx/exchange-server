use crate::{dto::UserRow, pool::pool};
use chrono::{DateTime, Utc};

pub async fn upsert_user(
    user_id: &str,
    username: &str,
    hashed_password: &str,
    salt: &str,
    created_at: DateTime<Utc>,
) -> Result<UserRow, sqlx::Error> {
    sqlx::query_as!(
        UserRow,
        r#"
    INSERT INTO users(user_id, username, hashed_password, salt,created_at)
    VALUES($1,$2,$3,$4,$5)
    ON CONFLICT (user_id)
    DO UPDATE
        SET username=EXCLUDED.username,
            hashed_password=EXCLUDED.hashed_password,
            salt=EXCLUDED.salt,
            updated_at=NOW()
    RETURNING *
    "#,
        user_id,
        username,
        hashed_password,
        salt,
        created_at
    )
    .fetch_one(pool())
    .await
}

pub async fn find_user_by_id(user_id: &str) -> Result<Option<UserRow>, sqlx::Error> {
    let user = sqlx::query_as!(UserRow, "SELECT * FROM users WHERE user_id = $1", user_id)
        .fetch_optional(pool())
        .await?;

    Ok(user)
}

pub async fn find_user_by_username(username: &str) -> Result<Option<UserRow>, sqlx::Error> {
    let user = sqlx::query_as!(UserRow, "SELECT * FROM users WHERE username = $1", username)
        .fetch_optional(pool())
        .await?;

    Ok(user)
}
