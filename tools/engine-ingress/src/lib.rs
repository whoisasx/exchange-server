use std::collections::BTreeMap;

use protocol::engine::{
    EngineCommand, FundingRateUpdatedInput, FundingSettlementTickInput, MarkPriceUpdatedInput,
};
use wallet::{
    engine_inputs::engine_command_outbox_message as wallet_engine_command_outbox_message,
    repository::NewWalletOutboxMessage,
};

pub fn parse_engine_command_json(input: &str) -> Result<EngineCommand, String> {
    let command = serde_json::from_str::<EngineCommand>(input)
        .map_err(|error| format!("invalid engine input JSON: {error}"))?;
    ensure_supported(command)
}

pub fn parse_engine_command_args<I, S>(args: I) -> Result<EngineCommand, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let subcommand = args
        .next()
        .ok_or_else(|| String::from("missing subcommand: expected mark-price, funding-rate, funding-settlement-tick, or json"))?;
    let flags = parse_flags(args)?;

    match subcommand.as_str() {
        "mark-price" => Ok(EngineCommand::MarkPriceUpdated(MarkPriceUpdatedInput {
            input_id: optional_string(&flags, "input-id"),
            market_id: required_i64(&flags, "market-id")?,
            mark_price: required_i64(&flags, "mark-price")?,
            index_price: required_i64(&flags, "index-price")?,
            source_timestamp_ms: required_i64(&flags, "source-timestamp-ms")?,
            published_at_ms: required_i64(&flags, "published-at-ms")?,
            valid_until_ms: required_i64(&flags, "valid-until-ms")?,
            source_sequence: required_i64(&flags, "source-sequence")?,
            source_status: required_string(&flags, "source-status")?,
        })),
        "funding-rate" => Ok(EngineCommand::FundingRateUpdated(FundingRateUpdatedInput {
            input_id: optional_string(&flags, "input-id"),
            market_id: required_i64(&flags, "market-id")?,
            funding_interval_id: required_string(&flags, "funding-interval-id")?,
            rate: required_i64(&flags, "rate")?,
            rate_scale: required_i64(&flags, "rate-scale")?,
            interval_start_ms: required_i64(&flags, "interval-start-ms")?,
            interval_end_ms: required_i64(&flags, "interval-end-ms")?,
            source_timestamp_ms: required_i64(&flags, "source-timestamp-ms")?,
        })),
        "funding-settlement-tick" => Ok(EngineCommand::FundingSettlementTick(
            FundingSettlementTickInput {
                input_id: optional_string(&flags, "input-id"),
                market_id: required_i64(&flags, "market-id")?,
                funding_interval_id: required_string(&flags, "funding-interval-id")?,
                settle_at_ms: required_i64(&flags, "settle-at-ms")?,
            },
        )),
        "json" => Err(String::from("json input must be read from stdin")),
        _ => Err(format!("unsupported subcommand '{subcommand}'")),
    }
}

pub fn command_name(command: &EngineCommand) -> &'static str {
    match command {
        EngineCommand::MarkPriceUpdated(_) => "MarkPriceUpdated",
        EngineCommand::FundingRateUpdated(_) => "FundingRateUpdated",
        EngineCommand::FundingSettlementTick(_) => "FundingSettlementTick",
        EngineCommand::PlaceOrder(_) => "PlaceOrder",
        EngineCommand::CancelOrder(_) => "CancelOrder",
        EngineCommand::LiquidatePosition(_) => "LiquidatePosition",
    }
}

pub fn engine_command_dedupe_key(command: &EngineCommand) -> Result<String, String> {
    match command {
        EngineCommand::MarkPriceUpdated(input) => Ok(input
            .input_id
            .as_ref()
            .map(|input_id| format!("engine-input:mark-price:{input_id}"))
            .unwrap_or_else(|| {
                format!(
                    "engine-input:mark-price:{}:{}:{}",
                    input.market_id, input.source_sequence, input.source_timestamp_ms
                )
            })),
        EngineCommand::FundingRateUpdated(input) => Ok(input
            .input_id
            .as_ref()
            .map(|input_id| format!("engine-input:funding-rate:{input_id}"))
            .unwrap_or_else(|| {
                format!(
                    "engine-input:funding-rate:{}:{}:{}",
                    input.market_id, input.funding_interval_id, input.source_timestamp_ms
                )
            })),
        EngineCommand::FundingSettlementTick(input) => Ok(input
            .input_id
            .as_ref()
            .map(|input_id| format!("engine-input:funding-settlement-tick:{input_id}"))
            .unwrap_or_else(|| {
                format!(
                    "engine-input:funding-settlement-tick:{}:{}:{}",
                    input.market_id, input.funding_interval_id, input.settle_at_ms
                )
            })),
        other => Err(format!(
            "unsupported engine input '{}'; engine-ingress only queues mark/funding inputs",
            command_name(other)
        )),
    }
}

pub fn engine_command_outbox_message(
    command: &EngineCommand,
    topic: &str,
) -> Result<NewWalletOutboxMessage, String> {
    let dedupe_key = engine_command_dedupe_key(command)?;

    wallet_engine_command_outbox_message(topic, dedupe_key, command)
        .map_err(|error| format!("failed to serialize engine input: {error:?}"))
}

fn ensure_supported(command: EngineCommand) -> Result<EngineCommand, String> {
    match command {
        EngineCommand::MarkPriceUpdated(_)
        | EngineCommand::FundingRateUpdated(_)
        | EngineCommand::FundingSettlementTick(_) => Ok(command),
        other => Err(format!(
            "unsupported engine input '{}'; engine-ingress only queues mark/funding inputs",
            command_name(&other)
        )),
    }
}

fn parse_flags<I>(args: I) -> Result<BTreeMap<String, String>, String>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut flags = BTreeMap::new();

    while let Some(flag) = args.next() {
        let Some(name) = flag.strip_prefix("--") else {
            return Err(format!("expected flag starting with '--', got '{flag}'"));
        };
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for '--{name}'"))?;
        if value.starts_with("--") {
            return Err(format!("missing value for '--{name}'"));
        }
        flags.insert(String::from(name), value);
    }

    Ok(flags)
}

fn required_string(flags: &BTreeMap<String, String>, name: &str) -> Result<String, String> {
    flags
        .get(name)
        .cloned()
        .ok_or_else(|| format!("missing required flag '--{name}'"))
}

fn optional_string(flags: &BTreeMap<String, String>, name: &str) -> Option<String> {
    flags.get(name).cloned()
}

fn required_i64(flags: &BTreeMap<String, String>, name: &str) -> Result<i64, String> {
    let value = required_string(flags, name)?;
    value
        .parse::<i64>()
        .map_err(|error| format!("invalid integer for '--{name}': {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mark_price_args() {
        let command = parse_engine_command_args([
            "mark-price",
            "--input-id",
            "mark-001",
            "--market-id",
            "1",
            "--mark-price",
            "100",
            "--index-price",
            "99",
            "--source-timestamp-ms",
            "1710000000000",
            "--published-at-ms",
            "1710000000100",
            "--valid-until-ms",
            "1710000005100",
            "--source-sequence",
            "45001",
            "--source-status",
            "VALID",
        ])
        .expect("mark price command should parse");

        assert_eq!(
            command,
            EngineCommand::MarkPriceUpdated(MarkPriceUpdatedInput {
                input_id: Some(String::from("mark-001")),
                market_id: 1,
                mark_price: 100,
                index_price: 99,
                source_timestamp_ms: 1_710_000_000_000,
                published_at_ms: 1_710_000_000_100,
                valid_until_ms: 1_710_000_005_100,
                source_sequence: 45_001,
                source_status: String::from("VALID"),
            })
        );
    }

    #[test]
    fn parses_funding_rate_args() {
        let command = parse_engine_command_args([
            "funding-rate",
            "--market-id",
            "1",
            "--funding-interval-id",
            "funding_SOL-PERP_1710000000_1710028800",
            "--rate",
            "25",
            "--rate-scale",
            "1000000",
            "--interval-start-ms",
            "1710000000000",
            "--interval-end-ms",
            "1710028800000",
            "--source-timestamp-ms",
            "1710000001000",
        ])
        .expect("funding rate command should parse");

        assert_eq!(
            command,
            EngineCommand::FundingRateUpdated(FundingRateUpdatedInput {
                input_id: None,
                market_id: 1,
                funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
                rate: 25,
                rate_scale: 1_000_000,
                interval_start_ms: 1_710_000_000_000,
                interval_end_ms: 1_710_028_800_000,
                source_timestamp_ms: 1_710_000_001_000,
            })
        );
    }

    #[test]
    fn parses_funding_settlement_tick_json() {
        let command = parse_engine_command_json(
            r#"{"type":"FundingSettlementTick","payload":{"input_id":"settle-001","market_id":1,"funding_interval_id":"funding_SOL-PERP_1710000000_1710028800","settle_at_ms":1710028800000}}"#,
        )
        .expect("funding settlement tick JSON should parse");

        assert_eq!(
            command,
            EngineCommand::FundingSettlementTick(FundingSettlementTickInput {
                input_id: Some(String::from("settle-001")),
                market_id: 1,
                funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
                settle_at_ms: 1_710_028_800_000,
            })
        );
    }

    #[test]
    fn rejects_order_json() {
        let error = parse_engine_command_json(
            r#"{"type":"CancelOrder","payload":{"envelope":{"request_id":"req","idempotency_key":"idem","user_id":1,"reply_partition":0},"market_id":1,"order_id":9}}"#,
        )
        .expect_err("order commands are outside this ingress path");

        assert!(error.contains("only queues mark/funding inputs"));
    }

    #[test]
    fn mark_price_dedupe_key_uses_input_id_when_present() {
        let command = EngineCommand::MarkPriceUpdated(MarkPriceUpdatedInput {
            input_id: Some(String::from("mark-001")),
            market_id: 1,
            mark_price: 100,
            index_price: 99,
            source_timestamp_ms: 1_710_000_000_000,
            published_at_ms: 1_710_000_000_100,
            valid_until_ms: 1_710_000_005_100,
            source_sequence: 45_001,
            source_status: String::from("VALID"),
        });

        assert_eq!(
            engine_command_dedupe_key(&command).expect("dedupe key should build"),
            "engine-input:mark-price:mark-001"
        );
    }

    #[test]
    fn funding_rate_dedupe_key_uses_source_identity_without_input_id() {
        let command = EngineCommand::FundingRateUpdated(FundingRateUpdatedInput {
            input_id: None,
            market_id: 1,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            rate: 25,
            rate_scale: 1_000_000,
            interval_start_ms: 1_710_000_000_000,
            interval_end_ms: 1_710_028_800_000,
            source_timestamp_ms: 1_710_000_001_000,
        });

        assert_eq!(
            engine_command_dedupe_key(&command).expect("dedupe key should build"),
            "engine-input:funding-rate:1:funding_SOL-PERP_1710000000_1710028800:1710000001000"
        );
    }

    #[test]
    fn outbox_message_targets_engine_input_topic() {
        let command = EngineCommand::FundingSettlementTick(FundingSettlementTickInput {
            input_id: Some(String::from("settle-001")),
            market_id: 1,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            settle_at_ms: 1_710_028_800_000,
        });

        let message = engine_command_outbox_message(&command, "engine.input")
            .expect("outbox message should serialize");

        assert_eq!(
            message.dedupe_key,
            "engine-input:funding-settlement-tick:settle-001"
        );
        assert_eq!(message.topic, "engine.input");
        assert_eq!(message.message_key, "1");
        assert_eq!(message.payload_type, "EngineCommand");
        assert_eq!(message.payload["type"], "FundingSettlementTick");
    }
}
