use std::error::Error;

use db::pool::{init_pool, run_migration};
use ledger::{
    processor::LedgerProcessor, repository::LedgerRepository, settings::LedgerSettings,
    worker::LedgerWorker,
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = LedgerSettings::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&settings.database_url)
        .await?;
    init_pool(pool.clone());
    run_migration().await?;

    let repository = LedgerRepository::new(pool);
    let processor = LedgerProcessor::new();
    let worker = LedgerWorker::new(settings, processor, repository);

    worker.run().await?;

    Ok(())
}
