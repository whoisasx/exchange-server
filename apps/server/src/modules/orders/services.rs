use db::{
    dto::OrderRow,
    orders::{get_market_orders, get_open_market_orders},
};

pub async fn get_users_market_all_orders(
    user_id: i64,
    market_id: i64,
) -> Result<Option<Vec<OrderRow>>, ()> {
    match get_market_orders(user_id, market_id).await {
        Ok(or) => Ok(Some(or)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(()),
    }
}

pub async fn get_all_open_orders(market_id: i64) -> Result<Option<Vec<OrderRow>>, ()> {
    match get_open_market_orders(market_id).await {
        Ok(or) => Ok(Some(or)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(()),
    }
}
