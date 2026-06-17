use std::error::Error;

use fake_engine::{engine::FakeEngine, redpanda::FakeEngineQueue, settings::FakeEngineSettings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = FakeEngineSettings::from_env();
    let engine = FakeEngine::new(settings.order_id_start, settings.fill_id_start);
    let queue = FakeEngineQueue::new(&settings).await?;

    println!(
        "fake engine starting: consuming '{}' and observing '{}'",
        settings.engine_commands_topic, settings.wallet_events_topic
    );

    queue.run(engine).await?;

    Ok(())
}
