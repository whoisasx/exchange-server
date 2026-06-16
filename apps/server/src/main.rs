use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use dotenvy::dotenv;

use config::Config;
use db::pool::{create_db_pool, init_pool, run_migration};

use crate::modules::{
    auth::{self},
    balances, fills, orders, positions, users,
};

mod middlewares;
mod modules;

pub mod utils;

pub async fn not_found() -> impl Responder {
    HttpResponse::NotFound().body("You sent a request to no where")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok().expect("CONFIG must be present");
    let config = Config::init();

    let pool = create_db_pool(&config.database_url)
        .await
        .expect("Database connection not established");
    init_pool(pool.clone());
    let _ = run_migration().await.expect("migrations failed");

    let port = config.server_port.clone();
    let host = config.server_host.clone();

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .wrap(cors)
            .service(
                web::scope("/api")
                    .configure(auth::routes::config_auth_routes)
                    .configure(balances::routes::config_balance_routes)
                    .configure(fills::routes::config_fills_routes)
                    .configure(orders::routes::config_orders_routes)
                    .configure(positions::routes::config_position_routes)
                    .configure(users::routes::config_user_routes),
            )
            .default_service(web::to(not_found))
            .app_data(pool.clone())
            .app_data(web::Data::new(config.clone()))
    })
    .shutdown_timeout(30)
    .bind((host, port))?
    .run()
    .await
}
