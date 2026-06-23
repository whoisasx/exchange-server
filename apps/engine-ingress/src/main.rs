use std::io::Read;

use engine_ingress::{command_name, parse_engine_command_args, parse_engine_command_json};
use protocol::engine::EngineCommand;
use wallet::{redpanda::WalletEngineInputPublisher, settings::WalletSettings};

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
    let publisher = WalletEngineInputPublisher::new(&settings)
        .await
        .map_err(|error| error.to_string())?;
    publish_command(&publisher, command).await?;

    println!(
        "published {command_name} to '{}'",
        settings.engine_commands_topic
    );

    Ok(())
}

async fn publish_command(
    publisher: &WalletEngineInputPublisher,
    command: EngineCommand,
) -> Result<(), String> {
    match command {
        EngineCommand::MarkPriceUpdated(input) => publisher
            .publish_mark_price_updated(input)
            .await
            .map_err(|error| error.to_string()),
        EngineCommand::FundingRateUpdated(input) => publisher
            .publish_funding_rate_updated(input)
            .await
            .map_err(|error| error.to_string()),
        EngineCommand::FundingSettlementTick(input) => publisher
            .publish_funding_settlement_tick(input)
            .await
            .map_err(|error| error.to_string()),
        other => Err(format!(
            "unsupported engine input '{}'; engine-ingress only publishes mark/funding inputs",
            command_name(&other)
        )),
    }
}
