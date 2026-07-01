use protocol::engine::{
    EngineCommand, FundingRateUpdatedInput, FundingSettlementTickInput, MarkPriceUpdatedInput,
};

use crate::repository::{NewWalletOutboxMessage, WalletRepositoryError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineInputPublication {
    key: String,
    input: EngineCommand,
}

impl EngineInputPublication {
    pub fn new(input: EngineCommand) -> Self {
        Self {
            key: engine_input_key(&input),
            input,
        }
    }

    pub fn mark_price_updated(input: MarkPriceUpdatedInput) -> Self {
        Self::new(EngineCommand::MarkPriceUpdated(input))
    }

    pub fn funding_rate_updated(input: FundingRateUpdatedInput) -> Self {
        Self::new(EngineCommand::FundingRateUpdated(input))
    }

    pub fn funding_settlement_tick(input: FundingSettlementTickInput) -> Self {
        Self::new(EngineCommand::FundingSettlementTick(input))
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn input(&self) -> &EngineCommand {
        &self.input
    }

    pub fn into_input(self) -> EngineCommand {
        self.input
    }
}

pub fn engine_input_key(input: &EngineCommand) -> String {
    engine_input_market_id(input).to_string()
}

fn engine_input_market_id(input: &EngineCommand) -> i64 {
    match input {
        EngineCommand::PlaceOrder(command) => command.market_id,
        EngineCommand::CancelOrder(command) => command.market_id,
        EngineCommand::LiquidatePosition(command) => command.market_id,
        EngineCommand::MarkPriceUpdated(command) => command.market_id,
        EngineCommand::FundingRateUpdated(command) => command.market_id,
        EngineCommand::FundingSettlementTick(command) => command.market_id,
    }
}

pub fn engine_command_outbox_message(
    topic: &str,
    dedupe_key: impl Into<String>,
    command: &EngineCommand,
) -> Result<NewWalletOutboxMessage, WalletRepositoryError> {
    let publication = EngineInputPublication::new(command.clone());

    NewWalletOutboxMessage::json(
        dedupe_key,
        topic,
        None,
        publication.key(),
        "EngineCommand",
        publication.input(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_price_update_publication_wraps_input_and_uses_market_id_key() {
        let input = MarkPriceUpdatedInput {
            input_id: Some(String::from("input_mark_001")),
            market_id: 7,
            mark_price: 100,
            index_price: 99,
            source_timestamp_ms: 1_710_000_000_000,
            published_at_ms: 1_710_000_000_100,
            valid_until_ms: 1_710_000_005_100,
            source_sequence: 45_001,
            source_status: String::from("VALID"),
        };

        let publication = EngineInputPublication::mark_price_updated(input.clone());

        assert_eq!(publication.key(), "7");
        assert_eq!(publication.input(), &EngineCommand::MarkPriceUpdated(input));
    }

    #[test]
    fn funding_rate_publication_wraps_input_and_uses_market_id_key() {
        let input = FundingRateUpdatedInput {
            input_id: Some(String::from("input_funding_rate_001")),
            market_id: 8,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            rate: 25,
            rate_scale: 1_000_000,
            interval_start_ms: 1_710_000_000_000,
            interval_end_ms: 1_710_028_800_000,
            source_timestamp_ms: 1_710_000_001_000,
        };

        let publication = EngineInputPublication::funding_rate_updated(input.clone());

        assert_eq!(publication.key(), "8");
        assert_eq!(
            publication.input(),
            &EngineCommand::FundingRateUpdated(input)
        );
    }

    #[test]
    fn funding_settlement_tick_publication_wraps_input_and_uses_market_id_key() {
        let input = FundingSettlementTickInput {
            input_id: Some(String::from("input_funding_settle_001")),
            market_id: 9,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            settle_at_ms: 1_710_028_800_000,
        };

        let publication = EngineInputPublication::funding_settlement_tick(input.clone());

        assert_eq!(publication.key(), "9");
        assert_eq!(
            publication.input(),
            &EngineCommand::FundingSettlementTick(input)
        );
    }

    #[test]
    fn publication_still_uses_market_id_without_input_id() {
        let publication =
            EngineInputPublication::funding_settlement_tick(FundingSettlementTickInput {
                input_id: None,
                market_id: 10,
                funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
                settle_at_ms: 1_710_028_800_000,
            });

        assert_eq!(publication.key(), "10");
    }

    #[test]
    fn market_id_key_is_stable_across_commands_for_same_market() {
        let mark = EngineCommand::MarkPriceUpdated(MarkPriceUpdatedInput {
            input_id: Some(String::from("mark-1")),
            market_id: 11,
            mark_price: 100,
            index_price: 99,
            source_timestamp_ms: 1_710_000_000_000,
            published_at_ms: 1_710_000_000_100,
            valid_until_ms: 1_710_000_005_100,
            source_sequence: 45_001,
            source_status: String::from("VALID"),
        });
        let funding = EngineCommand::FundingSettlementTick(FundingSettlementTickInput {
            input_id: Some(String::from("settle-1")),
            market_id: 11,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            settle_at_ms: 1_710_028_800_000,
        });

        assert_eq!(engine_input_key(&mark), "11");
        assert_eq!(engine_input_key(&funding), "11");
    }

    #[test]
    fn engine_command_outbox_message_wraps_command_payload() {
        let command = EngineCommand::FundingSettlementTick(FundingSettlementTickInput {
            input_id: Some(String::from("input_funding_settle_001")),
            market_id: 1,
            funding_interval_id: String::from("funding_SOL-PERP_1710000000_1710028800"),
            settle_at_ms: 1_710_028_800_000,
        });

        let message = engine_command_outbox_message(
            "engine.input",
            "engine-input:funding:settle-1",
            &command,
        )
        .expect("engine command outbox payload should serialize");

        assert_eq!(message.dedupe_key, "engine-input:funding:settle-1");
        assert_eq!(message.topic, "engine.input");
        assert_eq!(message.partition, None);
        assert_eq!(message.message_key, "1");
        assert_eq!(message.payload_type, "EngineCommand");
        assert_eq!(message.payload["type"], "FundingSettlementTick");
        assert_eq!(
            message.payload["payload"]["input_id"],
            "input_funding_settle_001"
        );
    }
}
