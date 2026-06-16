use db::{
    dto::UserRow,
    users::{find_user_by_username, upsert_user},
};

pub async fn is_user_exist(username: &str) -> Result<bool, ()> {
    match find_user_by_username(username).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(_) => Err(()),
    }
}
pub async fn register_user(
    user_id: i64,
    username: &str,
    hashed_password: &str,
) -> Result<UserRow, ()> {
    match upsert_user(user_id, username, hashed_password).await {
        Ok(u) => Ok(u),
        Err(_) => Err(()),
    }
}

pub async fn get_user_by_username(username: &str) -> Result<Option<UserRow>, ()> {
    match find_user_by_username(username).await {
        Ok(Some(u)) => Ok(Some(u)),
        Ok(None) => Ok(None),
        Err(_) => Err(()),
    }
}
