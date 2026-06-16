use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[sqlx(type_name = "side_type", rename_all = "UPPERCASE")]
pub enum SideType {
    LONG,
    SHORT,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[sqlx(type_name = "order_type", rename_all = "UPPERCASE")]
pub enum OrderType {
    LIMIT,
    MARKET,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[sqlx(type_name = "order_status", rename_all = "UPPERCASE")]
pub enum OrderStatus {
    PENDING,
    OPEN,
    FILLED,
    PARTIAL,
    CANCELLED,
    REJECTED,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[sqlx(type_name = "margin_type", rename_all = "UPPERCASE")]
pub enum MarginType {
    ISOLATED,
    CROSS,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[sqlx(type_name = "close_type", rename_all = "UPPERCASE")]
pub enum CloseType {
    TRADE,
    LIQUIDATION,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[sqlx(type_name = "asset_type", rename_all = "UPPERCASE")]
pub enum AssetType {
    USDC,
    USDT,
    SOL,
    ETH,
    BTC,
    PERP,
    HYP,
}
#[derive(sqlx::FromRow)]
pub struct UserRow {
    pub user_id: i64,
    pub username: String,
    pub hashed_password: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub struct MarketRow {
    pub market_id: i64,
    pub market_name: String,
    pub base_asset: AssetType,
    pub quote_asset: AssetType,
    pub decimal_base: i32,
    pub decimal_quote: i32,
    pub last_traded_price: i64,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct UserCollateralRow {
    pub user_id: i64,
    pub asset: AssetType,
    pub total: i64,
    pub locked: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct OrderRow {
    pub order_id: i64,
    pub user_id: i64,
    pub market_id: i64,
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

#[derive(Serialize)]
pub struct PositionRow {
    pub position_id: i64,
    pub user_id: i64,
    pub market_id: i64,
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

#[derive(Serialize)]
pub struct ClosedPositionRow {
    pub position_id: i64,
    pub user_id: i64,
    pub market_id: i64,
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
    pub open_order_id: i64,
    pub close_order_id: i64,
    pub close_reason: CloseType,
}

#[derive(Serialize)]
pub struct FillRow {
    pub fill_id: i64,
    pub maker_id: i64,
    pub taker_id: i64,
    pub maker_order_id: i64,
    pub taker_order_id: i64,
    pub price: i64,
    pub quantity: i64,
    pub maker_position: SideType,
    pub taker_position: SideType,
    pub created_at: Option<DateTime<Utc>>,
}
