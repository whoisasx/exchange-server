use db::dto::AssetType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DepositBalance {
    pub asset: AssetType,
    pub amount: i64,
    pub reference_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WithdrawBalance {
    pub asset: AssetType,
    pub amount: i64,
    pub destination: String,
}
