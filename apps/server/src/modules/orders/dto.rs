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

#[cfg(test)]
mod tests {
    use db::dto::OrderStatus;

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
}
