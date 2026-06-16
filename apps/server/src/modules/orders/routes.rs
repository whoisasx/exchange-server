use actix_web::{
    middleware::from_fn,
    web::{self, ServiceConfig},
};

use crate::{middlewares::auth, modules::orders::handlers};

pub fn config_orders_routes(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/orders")
            .service(handlers::get_open_orders)
            .service(
                web::scope("")
                    .wrap(from_fn(auth::auth_middleware))
                    .service(handlers::place_order)
                    .service(handlers::cancel_order)
                    .service(handlers::get_all_orders),
            ),
    );
}
