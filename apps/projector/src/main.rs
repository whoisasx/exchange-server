use std::error::Error;

use db::pool::{init_pool, run_migration};
use projector::{
    processor::ProjectorProcessor, repository::ProjectorRepository, settings::ProjectorSettings,
    worker::ProjectorWorker,
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = ProjectorSettings::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&settings.database_url)
        .await?;
    init_pool(pool.clone());
    run_migration().await?;

    let repository = ProjectorRepository::new(pool);
    let processor = ProjectorProcessor::new(repository.clone());
    let worker = ProjectorWorker::new(settings, processor, repository);

    worker.run().await?;

    Ok(())
}
