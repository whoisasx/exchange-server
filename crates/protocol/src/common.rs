use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Asset {
    #[serde(rename = "USDC")]
    USDC,
    #[serde(rename = "USDT")]
    USDT,
    #[serde(rename = "SOL")]
    SOL,
    #[serde(rename = "ETH")]
    ETH,
    #[serde(rename = "BTC")]
    BTC,
    #[serde(rename = "PERP")]
    PERP,
    #[serde(rename = "HYP")]
    HYP,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "LONG")]
    LONG,
    #[serde(rename = "SHORT")]
    SHORT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionSide {
    #[serde(rename = "LONG")]
    LONG,
    #[serde(rename = "SHORT")]
    SHORT,
    #[serde(rename = "FLAT")]
    FLAT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    #[serde(rename = "LIMIT")]
    LIMIT,
    #[serde(rename = "MARKET")]
    MARKET,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub request_id: String,
    pub idempotency_key: String,
    pub user_id: i64,
    pub reply_partition: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_json_names_stay_wire_compatible() {
        assert_eq!(serde_json::to_string(&Asset::USDC).unwrap(), "\"USDC\"");
        assert_eq!(serde_json::to_string(&Asset::PERP).unwrap(), "\"PERP\"");
        assert_eq!(
            serde_json::from_str::<Asset>("\"USDT\"").unwrap(),
            Asset::USDT
        );
    }

    #[test]
    fn side_json_names_stay_wire_compatible() {
        assert_eq!(serde_json::to_string(&Side::LONG).unwrap(), "\"LONG\"");
        assert_eq!(
            serde_json::from_str::<Side>("\"SHORT\"").unwrap(),
            Side::SHORT
        );
    }

    #[test]
    fn position_side_accepts_flat_wire_value() {
        assert_eq!(
            serde_json::from_str::<PositionSide>("\"FLAT\"").unwrap(),
            PositionSide::FLAT
        );
    }

    #[test]
    fn order_type_json_names_stay_wire_compatible() {
        assert_eq!(
            serde_json::to_string(&OrderType::LIMIT).unwrap(),
            "\"LIMIT\""
        );
        assert_eq!(
            serde_json::from_str::<OrderType>("\"MARKET\"").unwrap(),
            OrderType::MARKET
        );
    }
}
