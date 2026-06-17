use actix_web::{
    middleware::from_fn,
    web::{self, ServiceConfig},
};

use crate::{middlewares::auth, modules::balances::handlers};

pub fn config_balance_routes(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/balance")
            .wrap(from_fn(auth::auth_middleware))
            .service(handlers::add_balance)
            .service(handlers::withdraw_balance)
            .service(handlers::get_balance)
            .service(handlers::get_currency_balance),
    );
}
