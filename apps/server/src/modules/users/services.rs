use db::{
    dto::UserRow,
    users::{find_all_users, find_user_by_id},
};

pub async fn list_users() -> Result<Vec<UserRow>, ()> {
    match find_all_users().await {
        Ok(users) => Ok(users),
        Err(_) => Err(()),
    }
}

pub async fn get_user_by_userid(user_id: i64) -> Result<Option<UserRow>, ()> {
    match find_user_by_id(user_id).await {
        Ok(user) => Ok(user),
        Err(_) => Err(()),
    }
}
