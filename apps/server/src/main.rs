use actix_cors::Cors;
use actix_web::{App, HttpServer};
use dotenvy::dotenv;

use config::Config;
use db::pool::{create_db_pool, init_pool, run_migration};

mod middlewares;
mod modules;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok().expect("CONFIG must be present");
    let config = Config::init();

    let pool = create_db_pool(&config.database_url)
        .await
        .expect("Database connection not established");
    init_pool(pool);
    let _ = run_migration().await.expect("migrations failed");

    HttpServer::new(|| {
        let cors=Cors::permissive();

        App::new()
            .wrap(cors)
            .service()
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
