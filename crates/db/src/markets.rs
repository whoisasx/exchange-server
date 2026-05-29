use chrono::{DateTime, Utc};
use sqlx::postgres::PgQueryResult;

use crate::{dto::MarketRow, pool::pool};

pub async fn create_market(
    market_id: &str,
    market_name: &str,
    base_asset: &str,
    quote_asset: &str,
    decimal_base: i32,
    decimal_asset: i32,
    decimal_quant: i32,
    last_traded_price: i64,
    created_at: DateTime<Utc>,
) -> Result<MarketRow, sqlx::Error> {
    let market=sqlx::query_as!(
    MarketRow,
    r#"
    INSERT INTO markets(market_id,market_name,base_asset,quote_asset,decimal_base,decimal_asset, decimal_quant, last_traded_price, created_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
    RETURNING *
    "#,
    market_id,
    market_name,
    base_asset,
    quote_asset,
    decimal_base,
    decimal_asset,
    decimal_quant,
    last_traded_price,
    created_at
    )
    .fetch_one(pool())
    .await?;
    Ok(market)
}

pub async fn get_all_markets() -> Result<Vec<MarketRow>, sqlx::Error> {
    let markets = sqlx::query_as!(
        MarketRow,
        "SELECT
            market_id,
            market_name,
            base_asset,
            quote_asset,
            decimal_base,
            decimal_asset,
            decimal_quant,
            last_traded_price,
            created_at
    FROM markets"
    )
    .fetch_all(pool())
    .await?;

    Ok(markets)
}

pub async fn get_market_by_id(market_id: &str) -> Result<Option<MarketRow>, sqlx::Error> {
    let market = sqlx::query_as!(
        MarketRow,
        "SELECT * FROM markets WHERE market_id = $1",
        market_id
    )
    .fetch_optional(pool())
    .await?;

    Ok(market)
}
pub async fn get_market_by_market_name(
    market_name: &str,
) -> Result<Option<MarketRow>, sqlx::Error> {
    let market = sqlx::query_as!(
        MarketRow,
        "SELECT * FROM markets WHERE market_name= $1",
        market_name
    )
    .fetch_optional(pool())
    .await?;

    Ok(market)
}

pub async fn update_last_traded_price(
    market_id: &str,
    price: i64,
) -> Result<PgQueryResult, sqlx::Error> {
    let updated_market = sqlx::query!(
        r#"
        UPDATE markets
        SET last_traded_price=$1
        WHERE market_id=$2
    "#,
        price,
        market_id
    )
    .execute(pool())
    .await?;

    Ok(updated_market)
}
