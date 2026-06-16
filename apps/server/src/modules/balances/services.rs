use db::{
    collaterals::{get_collateral_by_asset, get_collaterals},
    dto::{AssetType, UserCollateralRow},
};

pub async fn get_user_balances(user_id: i64) -> Result<Option<Vec<UserCollateralRow>>, ()> {
    match get_collaterals(user_id).await {
        Ok(ub) => Ok(Some(ub)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(()),
    }
}

pub async fn get_user_asset_balances(
    user_id: i64,
    asset: AssetType,
) -> Result<Option<UserCollateralRow>, ()> {
    match get_collateral_by_asset(user_id, asset).await {
        Ok(ucb) => Ok(Some(ucb)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(_) => Err(()),
    }
}
