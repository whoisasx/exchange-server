use actix_web::{middleware::from_fn, web::{self}};
use crate::middlewares::auth;

use super::handlers;

pub fn config_user_routes(cfg: &mut web::ServiceConfig){
  cfg.service(
  web::scope("/users")
    .service(handlers::get_all_users)
    .wrap(from_fn(auth::auth_middleware))
    .service(handlers::get_user_details)
  );
}