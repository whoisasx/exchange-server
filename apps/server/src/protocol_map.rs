use db::dto::{AssetType, OrderType as DbOrderType, SideType};
use protocol::common::{Asset, OrderType, Side};

pub fn asset_to_protocol(asset: AssetType) -> Asset {
    match asset {
        AssetType::USDC => Asset::USDC,
        AssetType::USDT => Asset::USDT,
        AssetType::SOL => Asset::SOL,
        AssetType::ETH => Asset::ETH,
        AssetType::BTC => Asset::BTC,
        AssetType::PERP => Asset::PERP,
        AssetType::HYP => Asset::HYP,
    }
}

pub fn side_to_protocol(side: SideType) -> Side {
    match side {
        SideType::LONG => Side::LONG,
        SideType::SHORT => Side::SHORT,
    }
}

pub fn order_type_to_protocol(order_type: DbOrderType) -> OrderType {
    match order_type {
        DbOrderType::LIMIT => OrderType::LIMIT,
        DbOrderType::MARKET => OrderType::MARKET,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_mapping_covers_known_assets() {
        assert_eq!(asset_to_protocol(AssetType::USDC), Asset::USDC);
        assert_eq!(asset_to_protocol(AssetType::USDT), Asset::USDT);
        assert_eq!(asset_to_protocol(AssetType::SOL), Asset::SOL);
        assert_eq!(asset_to_protocol(AssetType::ETH), Asset::ETH);
        assert_eq!(asset_to_protocol(AssetType::BTC), Asset::BTC);
        assert_eq!(asset_to_protocol(AssetType::PERP), Asset::PERP);
        assert_eq!(asset_to_protocol(AssetType::HYP), Asset::HYP);
    }

    #[test]
    fn side_mapping_covers_known_sides() {
        assert_eq!(side_to_protocol(SideType::LONG), Side::LONG);
        assert_eq!(side_to_protocol(SideType::SHORT), Side::SHORT);
    }

    #[test]
    fn order_type_mapping_covers_known_types() {
        assert_eq!(order_type_to_protocol(DbOrderType::LIMIT), OrderType::LIMIT);
        assert_eq!(
            order_type_to_protocol(DbOrderType::MARKET),
            OrderType::MARKET
        );
    }
}
