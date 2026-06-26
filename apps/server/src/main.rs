use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use dotenvy::dotenv;

use config::Config;
use db::pool::{create_db_pool, init_pool, run_migration};

use crate::{
    redpanda::{RedpandaProducer, ReplyConsumers},
    replies::ReplyState,
};

use crate::modules::{
    auth::{self},
    balances, fills, markets, orders, positions, requests, users,
};

mod hot_path;
mod middlewares;
mod modules;
mod protocol_map;
mod redpanda;
mod replies;

pub mod utils;

pub async fn not_found() -> impl Responder {
    HttpResponse::NotFound().body("You sent a request to no where")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let config = Config::init();

    let pool = create_db_pool(&config.database_url)
        .await
        .expect("Database connection not established");
    init_pool(pool.clone());
    let _ = run_migration().await.expect("migrations failed");

    let port = config.server_port.clone();
    let host = config.server_host.clone();
    let redpanda_producer =
        RedpandaProducer::new(&config.redpanda_brokers, &config.wallet_commands_topic)
            .await
            .expect("Redpanda producer not initialized");
    let reply_state = ReplyState::default();
    let reply_consumers = ReplyConsumers::new(
        &config.redpanda_brokers,
        &config.wallet_replies_topic,
        &config.engine_replies_topic,
        config.server_reply_partition,
    )
    .await
    .expect("Redpanda reply consumers not initialized");
    reply_consumers.spawn(reply_state.clone());

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .wrap(cors)
            .service(
                web::scope("/api")
                    .configure(auth::routes::config_auth_routes)
                    .configure(balances::routes::config_balance_routes)
                    .configure(fills::routes::config_fills_routes)
                    .configure(markets::routes::config_market_routes)
                    .configure(orders::routes::config_orders_routes)
                    .configure(positions::routes::config_position_routes)
                    .configure(requests::routes::config_request_routes)
                    .configure(users::routes::config_user_routes),
            )
            .default_service(web::to(not_found))
            .app_data(pool.clone())
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(redpanda_producer.clone()))
            .app_data(web::Data::new(reply_state.clone()))
    })
    .shutdown_timeout(30)
    .bind((host, port))?
    .run()
    .await
}
