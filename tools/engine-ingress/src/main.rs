use std::io::Read;

use db::pool::{init_pool, run_migration};
use engine_ingress::{
    command_name, engine_command_outbox_message, parse_engine_command_args,
    parse_engine_command_json,
};
use sqlx::postgres::PgPoolOptions;
use wallet::{repository::WalletRepository, settings::WalletSettings};

#[tokio::main]
async fn main() -> Result<(), String> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let command = if args.first().is_some_and(|arg| arg == "json") {
        args.remove(0);
        if !args.is_empty() {
            return Err(String::from(
                "json input does not accept extra CLI arguments",
            ));
        }
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|error| format!("failed to read stdin: {error}"))?;
        parse_engine_command_json(&input)?
    } else {
        parse_engine_command_args(args)?
    };

    let command_name = command_name(&command);
    let settings = WalletSettings::from_env();
    let outbox_message = engine_command_outbox_message(&command, &settings.engine_input_topic)?;
    let dedupe_key = outbox_message.dedupe_key.clone();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database_url)
        .await
        .map_err(|error| format!("failed to connect to database: {error}"))?;
    init_pool(pool.clone());
    run_migration()
        .await
        .map_err(|error| format!("failed to run database migrations: {error}"))?;

    let repository = WalletRepository::new(pool);
    let outbox_id = repository
        .enqueue_outbox_message(&outbox_message)
        .await
        .map_err(|error| format!("failed to enqueue wallet outbox message: {error:?}"))?;

    if let Some(outbox_id) = outbox_id {
        println!(
            "queued {command_name} to wallet_outbox row {outbox_id} for '{}'",
            settings.engine_input_topic
        );
    } else {
        println!(
            "engine input already queued for dedupe key '{dedupe_key}' on '{}'",
            settings.engine_input_topic
        );
    }

    Ok(())
}
