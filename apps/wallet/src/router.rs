use protocol::wallet::{
    CancelOrderIntent, Deposit, PlaceOrderIntent, ReleaseReservation, SettleTrade, WalletCommand,
    Withdraw,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletAction {
    ReserveAndForward(PlaceOrderIntent),
    ForwardCancel(CancelOrderIntent),
    ApplyDeposit(Deposit),
    ApplyWithdrawal(Withdraw),
    ReleaseReservation(ReleaseReservation),
    SettleTrade(SettleTrade),
}

pub fn route_command(command: WalletCommand) -> WalletAction {
    match command {
        WalletCommand::PlaceOrderIntent(intent) => WalletAction::ReserveAndForward(intent),
        WalletCommand::CancelOrderIntent(intent) => WalletAction::ForwardCancel(intent),
        WalletCommand::Deposit(deposit) => WalletAction::ApplyDeposit(deposit),
        WalletCommand::Withdraw(withdraw) => WalletAction::ApplyWithdrawal(withdraw),
        WalletCommand::ReleaseReservation(release) => WalletAction::ReleaseReservation(release),
        WalletCommand::SettleTrade(settle) => WalletAction::SettleTrade(settle),
    }
}

#[cfg(test)]
mod tests {
    use protocol::{
        common::{Asset, CommandEnvelope, OrderType, Side},
        wallet::{CancelOrderIntent, PlaceOrderIntent, WalletCommand},
    };

    use super::*;

    #[test]
    fn place_order_intent_routes_to_reserve_and_forward() {
        let intent = PlaceOrderIntent {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("order-1"),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 1,
            market_name: String::from("SOL-PERP"),
            side: Side::LONG,
            order_type: OrderType::LIMIT,
            quantity: 10,
            price: 20,
            margin_asset: Asset::USDC,
            required_margin: 200,
        };

        let action = route_command(WalletCommand::PlaceOrderIntent(intent));

        assert!(matches!(action, WalletAction::ReserveAndForward(_)));
    }

    #[test]
    fn cancel_order_intent_routes_to_forward_cancel() {
        let intent = CancelOrderIntent {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("cancel-1"),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 1,
            order_id: 99,
        };

        let action = route_command(WalletCommand::CancelOrderIntent(intent));

        assert!(matches!(action, WalletAction::ForwardCancel(_)));
    }
}
