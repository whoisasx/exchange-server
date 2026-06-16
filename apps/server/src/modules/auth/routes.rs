use actix_web::web::{self, ServiceConfig};

use crate::modules::auth::handlers;

pub fn config_auth_routes(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/auth")
            .service(handlers::signup_user)
            .service(handlers::signin_user),
    );
}
