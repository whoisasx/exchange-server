use std::sync::OnceLock;

use sqlx::{
    Pool,
    migrate::MigrateError,
    postgres::{PgPoolOptions, Postgres},
};

static DB_POOL: OnceLock<Pool<Postgres>> = OnceLock::new();

pub async fn create_db_pool(database_url: &str) -> Result<Pool<Postgres>, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
}

pub fn init_pool(pool: Pool<Postgres>) {
    match DB_POOL.set(pool) {
        Ok(()) => {}
        Err(_) => panic!("db_pool already exists"),
    }
}

pub fn pool() -> &'static Pool<Postgres> {
    match DB_POOL.get() {
        Some(pool) => pool,
        None => panic!("db_pool is not initialized"),
    }
}

pub async fn run_migration() -> Result<(), MigrateError> {
    sqlx::migrate!("./migrations").run(pool()).await
}
