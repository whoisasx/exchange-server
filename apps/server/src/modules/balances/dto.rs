use db::dto::AssetType;
use serde::Deserialize;

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct BalanceIn {
    asset: AssetType,
    amount: i64,
}
