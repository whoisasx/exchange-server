use actix_web::web;

use crate::modules::markets::handlers;

pub fn config_market_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/markets")
            .service(handlers::get_market_candles)
            .service(handlers::get_orderbook_snapshot),
    );
}
