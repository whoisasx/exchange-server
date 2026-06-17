use actix_web::{
    middleware::from_fn,
    web::{self, ServiceConfig},
};

use crate::{middlewares::auth, modules::requests::handlers};

pub fn config_request_routes(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/requests")
            .wrap(from_fn(auth::auth_middleware))
            .service(handlers::get_request_status),
    );
}
