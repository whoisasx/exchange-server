use actix_web::{middleware::from_fn, web};

use crate::{middlewares::auth, modules::fills::handlers};

pub fn config_fills_routes(cfg: &mut web::ServiceConfig){
  cfg.service(
    web::scope("/fills")
        .wrap(from_fn(auth::auth_middleware))
        .service(handlers::get_fills)
  );
}