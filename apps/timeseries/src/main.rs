use std::error::Error;

use db::pool::{init_pool, run_migration};
use sqlx::postgres::PgPoolOptions;
use timeseries::{
    processor::TimeseriesProcessor, repository::TimeseriesRepository, settings::TimeseriesSettings,
    worker::TimeseriesWorker,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = TimeseriesSettings::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&settings.database_url)
        .await?;
    init_pool(pool.clone());
    run_migration().await?;

    let repository = TimeseriesRepository::new(pool);
    let processor = TimeseriesProcessor::new();
    let worker = TimeseriesWorker::new(settings, processor, repository);

    worker.run().await?;

    Ok(())
}
