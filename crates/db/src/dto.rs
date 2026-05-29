use chrono::{DateTime, Utc};
use sqlx::types::BigDecimal;

pub enum SideType {
    LONG,
    SHORT,
}
pub enum OrderType {
    LIMIT,
    MARKET,
}
pub enum OrderStatus {
    PENDING,
    OPEN,
    FILLED,
    PARTIAL,
    CANCELLED,
    REJECTED,
}
pub enum MarginType {
    ISOLATED,
    CROSS,
}
pub enum CloseType {
    TRADE,
    LIQUIDATION,
}
pub struct UserRow {
    pub user_id: String,
    pub username: String,
    pub hashed_password: Box<String>,
    pub salt: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct MarketRow {
    pub market_id: String,
    pub market_name: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub decimal_base: i32,
    pub decimal_asset: i32,
    pub decimal_quant: i32,
    pub last_traded_price: BigDecimal,
    pub created_at: Option<DateTime<Utc>>,
}

pub struct UserCollateralRow {
    pub user_id: String,
    pub asset: String,
    pub total: u128,
    pub locked: u128,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct OrderRow {
    pub order_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub order_type: OrderType,
    pub quantity: u128,
    pub price: u128,
    pub status: OrderStatus,
    pub margin: u128,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct PositionRow {
    pub position_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub quantity: u128,
    pub unrealized_pnl: u128,
    pub maintenance_margin: u128,
    pub initial_margin: u128,
    pub margin_chosen: MarginType,
    pub liquidation_price: u128,
    pub average_price: u128,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct ClosedPositionRow {
    pub position_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub quantity: u128,
    pub entry_price: u128,
    pub exit_price: u128,
    pub realized_pnl: u128,
    pub initial_margin: u128,
    pub closing_fee: u128,
    pub opened_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub open_order_id: String,
    pub close_order_id: String,
    pub close_reason: CloseType,
}

pub struct FillRow {
    pub fill_id: String,
    pub maker_id: String,
    pub taker_id: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub price: u128,
    pub quantity: u128,
    pub maker_position: SideType,
    pub taker_position: SideType,
    pub created_at: Option<DateTime<Utc>>,
}
