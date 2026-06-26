use std::error::Error;

use db::pool::{init_pool, run_migration};
use sqlx::postgres::PgPoolOptions;
use wallet::{
    processor::WalletProcessor, repository::WalletRepository, settings::WalletSettings,
    worker::WalletWorker,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = WalletSettings::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&settings.database_url)
        .await?;
    init_pool(pool.clone());
    run_migration().await?;

    let repository = WalletRepository::new(pool);
    let processor = WalletProcessor::new_with_topics(
        repository.clone(),
        settings.wallet_events_topic.clone(),
        settings.engine_input_topic.clone(),
    );
    let worker = WalletWorker::new(settings, processor, repository);

    worker.run().await?;

    Ok(())
}
