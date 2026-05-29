use chrono::{DateTime, Utc};

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "side_type", rename_all = "UPPERCASE")]
pub enum SideType {
    LONG,
    SHORT,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "order_type", rename_all = "UPPERCASE")]
pub enum OrderType {
    LIMIT,
    MARKET,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "order_status", rename_all = "UPPERCASE")]
pub enum OrderStatus {
    PENDING,
    OPEN,
    FILLED,
    PARTIAL,
    CANCELLED,
    REJECTED,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "margin_type", rename_all = "UPPERCASE")]
pub enum MarginType {
    ISOLATED,
    CROSS,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "close_type", rename_all = "UPPERCASE")]
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
    pub last_traded_price: i64,
    pub created_at: Option<DateTime<Utc>>,
}

pub struct UserCollateralRow {
    pub user_id: String,
    pub asset: String,
    pub total: i64,
    pub locked: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct OrderRow {
    pub order_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
    pub status: OrderStatus,
    pub margin: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct PositionRow {
    pub position_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub quantity: i64,
    pub unrealized_pnl: i64,
    pub maintenance_margin: i64,
    pub initial_margin: i64,
    pub margin_chosen: MarginType,
    pub liquidation_price: i64,
    pub average_price: i64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct ClosedPositionRow {
    pub position_id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: SideType,
    pub quantity: i64,
    pub entry_price: i64,
    pub exit_price: i64,
    pub realized_pnl: i64,
    pub initial_margin: i64,
    pub closing_fee: i64,
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
    pub price: i64,
    pub quantity: i64,
    pub maker_position: SideType,
    pub taker_position: SideType,
    pub created_at: Option<DateTime<Utc>>,
}
