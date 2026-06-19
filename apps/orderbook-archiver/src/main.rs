use std::error::Error;

use orderbook_archiver::{
    archive::{LocalArchiveStore, LocalOffsetStore},
    settings::OrderbookArchiverSettings,
    worker::OrderbookArchiverWorker,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = OrderbookArchiverSettings::from_env();
    let archive_store = LocalArchiveStore::new(settings.archive_local_root.clone());
    let offset_store = LocalOffsetStore::new(settings.offset_local_root.clone());
    let worker = OrderbookArchiverWorker::new(settings, archive_store, offset_store);

    worker.run().await?;

    Ok(())
}
