use db::dto::{AssetType, OrderRow, OrderType, SideType};
use serde::{Deserialize, Serialize};

pub const DEFAULT_LEVERAGE: i64 = 1;

#[derive(Deserialize)]
pub struct PlaceOrder {
    pub market_id: i64,
    pub market_name: String,
    pub side: SideType,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
    pub margin: i64,
    #[serde(default = "default_margin_asset")]
    pub margin_asset: AssetType,
    #[serde(default = "default_leverage")]
    pub leverage: i64,
}

#[derive(Deserialize)]
pub struct CancelOrder {
    pub market_id: i64,
    pub order_id: i64,
}

fn default_margin_asset() -> AssetType {
    AssetType::USDC
}

fn default_leverage() -> i64 {
    DEFAULT_LEVERAGE
}

#[derive(Serialize)]
pub struct PublicOpenOrder {
    pub market_id: i64,
    pub market_name: String,
    pub side: SideType,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
}

impl From<OrderRow> for PublicOpenOrder {
    fn from(order: OrderRow) -> Self {
        Self {
            market_id: order.market_id,
            market_name: order.market_name,
            side: order.side,
            order_type: order.order_type,
            quantity: order.quantity,
            price: order.price,
        }
    }
}

#[cfg(test)]
mod tests {
    use db::dto::OrderStatus;
    use serde_json::json;

    use super::*;

    #[test]
    fn public_open_order_excludes_private_order_fields() {
        let order = OrderRow {
            order_id: 99,
            user_id: 42,
            market_id: 1,
            market_name: String::from("SOL-PERP"),
            side: SideType::LONG,
            order_type: OrderType::LIMIT,
            quantity: 10,
            price: 100,
            status: OrderStatus::OPEN,
            margin: 50,
            created_at: None,
            updated_at: None,
        };

        let public = PublicOpenOrder::from(order);

        assert_eq!(public.market_id, 1);
        assert_eq!(public.market_name, "SOL-PERP");
        assert_eq!(public.side, SideType::LONG);
        assert_eq!(public.order_type, OrderType::LIMIT);
        assert_eq!(public.quantity, 10);
        assert_eq!(public.price, 100);
    }

    #[test]
    fn place_order_defaults_leverage_for_legacy_requests() {
        let order: PlaceOrder = serde_json::from_value(json!({
            "market_id": 1,
            "market_name": "SOL-PERP",
            "side": "LONG",
            "order_type": "LIMIT",
            "quantity": 10,
            "price": 100,
            "margin": 1000
        }))
        .expect("legacy order request should deserialize");

        assert_eq!(order.leverage, DEFAULT_LEVERAGE);
        assert_eq!(order.margin_asset, AssetType::USDC);
    }
}
