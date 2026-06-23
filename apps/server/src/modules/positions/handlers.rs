use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, post, web};
use db::dto::{AssetType, ClosedPositionRow, PositionRow, SideType};
use protocol::{
    common::{OrderType as ProtocolOrderType, Side},
    wallet::{PlaceOrderIntent, WalletCommand},
};
use serde::Deserialize;

use crate::{
    hot_path::{
        bad_request, command_context, final_or_queued_response, internal_server_error,
        redpanda_producer, reply_state,
    },
    modules::{
        auth::dto::Claim,
        orders::services::allocate_order_id,
        positions::services::{get_user_closed_positions, get_user_open_position},
    },
    protocol_map::asset_to_protocol,
    replies::RequestKind,
    utils::types::ResponseBody,
};

const DEFAULT_CLOSE_LEVERAGE: i64 = 1;
const DEFAULT_CLOSE_MARGIN: i64 = 1;
const MIN_LEVERAGE: i64 = 1;
const MAX_LEVERAGE: i64 = 100;

#[derive(Deserialize)]
pub struct ClosePositionRequest {
    pub market_id: i64,
    pub quantity: Option<i64>,
    pub price: Option<i64>,
    pub margin: Option<i64>,
    #[serde(default = "default_margin_asset")]
    pub margin_asset: AssetType,
    #[serde(default = "default_close_leverage")]
    pub leverage: i64,
}

fn default_margin_asset() -> AssetType {
    AssetType::USDC
}

fn default_close_leverage() -> i64 {
    DEFAULT_CLOSE_LEVERAGE
}

fn authenticated_user(req: &HttpRequest) -> Result<Claim, HttpResponse> {
    let extensions = req.extensions();
    match extensions.get::<Claim>() {
        Some(claim) => Ok(claim.clone()),
        None => Err(HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
            success: false,
            info: String::from("user not authenticated."),
            body: None,
        })),
    }
}

#[get("/open/{market_id}")]
pub async fn get_open_positions(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
    let claim = match authenticated_user(&req) {
        Ok(claim) => claim,
        Err(response) => return response,
    };

    match get_user_open_position(claim.userid, path.into_inner()).await {
        Ok(Some(position)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("open position fetched"),
            body: Some(position),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<PositionRow> {
            success: true,
            info: String::from("no open position"),
            body: None,
        }),
        Err(_) => HttpResponse::InternalServerError().json(ResponseBody::<PositionRow> {
            success: false,
            info: String::from("internal server error"),
            body: None,
        }),
    }
}

pub async fn get_closed_positions(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
    let claim = match authenticated_user(&req) {
        Ok(claim) => claim,
        Err(response) => return response,
    };

    match get_user_closed_positions(claim.userid, path.into_inner()).await {
        Ok(positions) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("closed positions fetched"),
            body: Some(positions),
        }),
        Err(_) => {
            HttpResponse::InternalServerError().json(ResponseBody::<Vec<ClosedPositionRow>> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

#[post("/close")]
pub async fn close_position(
    req: HttpRequest,
    body: web::Json<ClosePositionRequest>,
) -> impl Responder {
    let claim = match authenticated_user(&req) {
        Ok(claim) => claim,
        Err(response) => return response,
    };

    let request = body.into_inner();
    if request.market_id <= 0 {
        return bad_request("market_id must be greater than zero");
    }

    let position = match get_user_open_position(claim.userid, request.market_id).await {
        Ok(Some(position)) => position,
        Ok(None) => return bad_request("no open position for market"),
        Err(_) => return internal_server_error("failed to load open position"),
    };

    let quantity = request.quantity.unwrap_or(position.quantity);
    if quantity <= 0 {
        return bad_request("quantity must be greater than zero");
    }
    if quantity > position.quantity {
        return bad_request("close quantity cannot exceed open position quantity");
    }

    let price = request.price.unwrap_or(0);
    if price < 0 {
        return bad_request("price cannot be negative");
    }

    let required_margin = match close_required_margin(request.margin) {
        Ok(margin) => margin,
        Err(message) => return bad_request(message),
    };
    if let Err(message) = validate_close_leverage(request.leverage) {
        return bad_request(message);
    }

    let closing_side = match position.side {
        SideType::LONG => Side::SHORT,
        SideType::SHORT => Side::LONG,
    };
    let order_type = if price == 0 {
        ProtocolOrderType::MARKET
    } else {
        ProtocolOrderType::LIMIT
    };

    let context = match command_context(&req, &claim) {
        Ok(context) => context,
        Err(response) => return response,
    };
    let producer = match redpanda_producer(&req) {
        Ok(producer) => producer,
        Err(response) => return response,
    };
    let reply_state = match reply_state(&req) {
        Ok(reply_state) => reply_state,
        Err(response) => return response,
    };
    let order_id = match allocate_order_id().await {
        Ok(order_id) => order_id,
        Err(error) => {
            eprintln!("{error}");
            return internal_server_error("failed to allocate order id");
        }
    };

    let command = WalletCommand::PlaceOrderIntent(PlaceOrderIntent {
        envelope: context.envelope,
        order_id,
        market_id: position.market_id,
        market_name: position.market_name,
        side: closing_side,
        order_type,
        quantity,
        price,
        margin_asset: asset_to_protocol(request.margin_asset),
        required_margin,
        leverage: request.leverage,
        reduce_only: true,
    });
    let key = claim.userid.to_string();
    let receiver = reply_state
        .register_waiter(
            &context.ack.request_id,
            claim.userid,
            RequestKind::PlaceOrder,
        )
        .await;

    if let Err(error) = producer.publish_wallet_command(&key, &command).await {
        eprintln!("{error}");
        reply_state.remove(&context.ack.request_id).await;
        return internal_server_error("failed to queue wallet command");
    }

    final_or_queued_response(receiver, context.wait_timeout, context.ack).await
}

fn close_required_margin(margin: Option<i64>) -> Result<i64, String> {
    let margin = margin.unwrap_or(DEFAULT_CLOSE_MARGIN);
    if margin <= 0 {
        return Err(String::from("margin must be greater than zero"));
    }
    Ok(margin)
}

fn validate_close_leverage(leverage: i64) -> Result<(), String> {
    if !(MIN_LEVERAGE..=MAX_LEVERAGE).contains(&leverage) {
        return Err(format!(
            "leverage must be between {MIN_LEVERAGE} and {MAX_LEVERAGE}"
        ));
    }
    Ok(())
}

#[post("/liquidate")]
pub async fn liquidate_position(req: HttpRequest) -> impl Responder {
    if let Err(response) = authenticated_user(&req) {
        return response;
    }

    HttpResponse::NotImplemented().json(ResponseBody::<()> {
        success: false,
        info: String::from("liquidation command is not implemented yet"),
        body: None,
    })
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use serde_json::json;

    use super::*;

    #[actix_web::test]
    async fn get_open_positions_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_open_positions)).await;
        let req = test::TestRequest::get().uri("/open/1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_closed_positions_rejects_requests_without_claim() {
        let app = test::init_service(
            App::new().route("/closed/{market_id}", web::get().to(get_closed_positions)),
        )
        .await;
        let req = test::TestRequest::get().uri("/closed/1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn close_position_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(close_position)).await;
        let req = test::TestRequest::post()
            .uri("/close")
            .set_json(json!({"market_id":1}))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn close_position_request_defaults_match_protocol_order_defaults() {
        let request: ClosePositionRequest =
            serde_json::from_value(json!({"market_id": 1})).expect("request should deserialize");

        assert_eq!(request.leverage, DEFAULT_CLOSE_LEVERAGE);
        assert_eq!(
            close_required_margin(request.margin),
            Ok(DEFAULT_CLOSE_MARGIN)
        );
    }

    #[actix_web::test]
    async fn close_position_rejects_non_positive_margin() {
        let zero = close_required_margin(Some(0)).expect_err("margin should be invalid");
        let negative = close_required_margin(Some(-1)).expect_err("margin should be invalid");

        assert_eq!(zero, "margin must be greater than zero");
        assert_eq!(negative, "margin must be greater than zero");
    }

    #[actix_web::test]
    async fn close_position_rejects_invalid_leverage() {
        let error =
            validate_close_leverage(MAX_LEVERAGE + 1).expect_err("leverage should be invalid");

        assert_eq!(error, "leverage must be between 1 and 100");
    }
}
