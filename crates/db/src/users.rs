use crate::{dto::UserRow, pool::pool};

pub async fn upsert_user(
    user_id: i64,
    username: &str,
    hashed_password: &str,
) -> Result<UserRow, sqlx::Error> {
    sqlx::query_as!(
        UserRow,
        r#"
    INSERT INTO users(user_id, username, hashed_password)
    VALUES($1,$2,$3)
    ON CONFLICT (user_id)
    DO UPDATE
        SET username=EXCLUDED.username,
            hashed_password=EXCLUDED.hashed_password,
            updated_at=NOW()
    RETURNING *
    "#,
        user_id,
        username,
        hashed_password
    )
    .fetch_one(pool())
    .await
}

pub async fn find_user_by_id(user_id: i64) -> Result<Option<UserRow>, sqlx::Error> {
    let user = sqlx::query_as!(UserRow, "SELECT * FROM users WHERE user_id = $1", user_id)
        .fetch_optional(pool())
        .await?;

    Ok(user)
}

pub async fn find_all_users() -> Result<Vec<UserRow>, sqlx::Error> {
    let users = sqlx::query_as::<_, UserRow>(
        r#"
        SELECT user_id, username, hashed_password, created_at, updated_at
        FROM users
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool())
    .await?;

    Ok(users)
}

pub async fn find_user_by_username(username: &str) -> Result<Option<UserRow>, sqlx::Error> {
    let user = sqlx::query_as!(UserRow, "SELECT * FROM users WHERE username = $1", username)
        .fetch_optional(pool())
        .await?;

    Ok(user)
}
