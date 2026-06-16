use actix_web::{middleware::from_fn, web};

use crate::{middlewares::auth, modules::fills::handlers};

pub fn config_fills_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/fills")
            .wrap(from_fn(auth::auth_middleware))
            .service(handlers::get_fills)
            .service(handlers::get_orders_fills)
            .route(
                "/positions/{position_id}",
                web::get().to(handlers::get_positions_fills),
            )
            .route(
                "/postions/{position_id}",
                web::get().to(handlers::get_positions_fills),
            )
            .service(handlers::get_closed_positions_fills),
    );
}
