use actix_web::{
    middleware::from_fn,
    web::{self},
};

use crate::{middlewares::auth, modules::positions::handlers};

pub fn config_position_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/positions")
            .wrap(from_fn(auth::auth_middleware))
            .service(handlers::get_open_positions)
            .route(
                "/closed/{market_id}",
                web::get().to(handlers::get_closed_positions),
            )
            .service(handlers::close_position),
    );
}
