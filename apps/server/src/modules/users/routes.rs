use crate::middlewares::auth;
use actix_web::{
    middleware::from_fn,
    web::{self},
};

use super::handlers;

pub fn config_user_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/users")
            .wrap(from_fn(auth::auth_middleware))
            .service(handlers::get_all_users)
            .service(handlers::get_user_details),
    );
}
