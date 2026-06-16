use db::{
    closed_positions::get_closed_positions_by_user_market,
    dto::{ClosedPositionRow, PositionRow},
    positions::get_position_by_user_market,
};

pub async fn get_user_open_position(
    user_id: i64,
    market_id: i64,
) -> Result<Option<PositionRow>, ()> {
    match get_position_by_user_market(user_id, market_id).await {
        Ok(position) => Ok(position),
        Err(_) => Err(()),
    }
}

pub async fn get_user_closed_positions(
    user_id: i64,
    market_id: i64,
) -> Result<Vec<ClosedPositionRow>, ()> {
    match get_closed_positions_by_user_market(user_id, market_id).await {
        Ok(positions) => Ok(positions),
        Err(_) => Err(()),
    }
}
