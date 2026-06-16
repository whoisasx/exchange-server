use db::dto::{OrderRow, OrderType, SideType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct PlaceOrder {
    pub market_id: i64,
    pub market_name: String,
    pub side: SideType,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
    pub margin: i64,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct CancelOrder {
    pub market_id: i64,
    pub order_id: i64,
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
