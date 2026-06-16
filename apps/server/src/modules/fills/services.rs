use db::{
    closed_positions::get_closed_position_by_id,
    dto::FillRow,
    fills::{
        get_fills_by_closed_position_id, get_fills_by_order_id, get_fills_by_position_id,
        get_fills_by_user_id,
    },
    orders::get_order_by_order_id,
    positions::get_position_by_id,
};

#[derive(Debug)]
pub enum FillServiceError {
    Forbidden,
    Storage,
}

pub async fn get_user_fills(user_id: i64) -> Result<Option<Vec<FillRow>>, FillServiceError> {
    match get_fills_by_user_id(user_id).await {
        Ok(uf) => Ok(Some(uf)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(FillServiceError::Storage),
    }
}

pub async fn get_order_id_fills(
    user_id: i64,
    order_id: i64,
) -> Result<Option<Vec<FillRow>>, FillServiceError> {
    let order = match get_order_by_order_id(order_id).await {
        Ok(order) => order,
        Err(sqlx::Error::RowNotFound) => return Ok(None),
        Err(_) => return Err(FillServiceError::Storage),
    };

    if order.user_id != user_id {
        return Err(FillServiceError::Forbidden);
    }

    match get_fills_by_order_id(order_id).await {
        Ok(uf) => Ok(Some(uf)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(FillServiceError::Storage),
    }
}

pub async fn get_position_id_fills(
    user_id: i64,
    position_id: i64,
) -> Result<Option<Vec<FillRow>>, FillServiceError> {
    let position = match get_position_by_id(position_id).await {
        Ok(Some(position)) => position,
        Ok(None) => return Ok(None),
        Err(_) => return Err(FillServiceError::Storage),
    };

    if position.user_id != user_id {
        return Err(FillServiceError::Forbidden);
    }

    match get_fills_by_position_id(position_id).await {
        Ok(uf) => Ok(Some(uf)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(FillServiceError::Storage),
    }
}

pub async fn get_position_closed_id_fills(
    user_id: i64,
    closed_position_id: i64,
) -> Result<Option<Vec<FillRow>>, FillServiceError> {
    let position = match get_closed_position_by_id(closed_position_id).await {
        Ok(Some(position)) => position,
        Ok(None) => return Ok(None),
        Err(_) => return Err(FillServiceError::Storage),
    };

    if position.user_id != user_id {
        return Err(FillServiceError::Forbidden);
    }

    match get_fills_by_closed_position_id(closed_position_id).await {
        Ok(uf) => Ok(Some(uf)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(FillServiceError::Storage),
    }
}
