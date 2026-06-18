use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, delete, get, post, web};
use db::dto::OrderRow;
use protocol::wallet::{CancelOrderIntent, PlaceOrderIntent, WalletCommand};

use crate::{
    hot_path::{
        bad_request, command_context, final_or_queued_response, internal_server_error,
        redpanda_producer, reply_state,
    },
    modules::{
        auth::dto::Claim,
        orders::{
            dto::{CancelOrder, PlaceOrder, PublicOpenOrder},
            services::{get_all_open_orders, get_users_market_all_orders},
        },
    },
    protocol_map::{asset_to_protocol, order_type_to_protocol, side_to_protocol},
    replies::RequestKind,
    utils::types::ResponseBody,
};

#[post("/")]
pub async fn place_order(req: HttpRequest, body: web::Json<PlaceOrder>) -> impl Responder {
    let extensions = req.extensions();
    let user_extension = match extensions.get::<Claim>() {
        Some(ue) => ue,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authorized"),
                body: None,
            });
        }
    };

    let order_data = body.into_inner();
    if order_data.market_id <= 0 {
        return bad_request("market_id must be greater than zero");
    }
    if order_data.quantity <= 0 {
        return bad_request("quantity must be greater than zero");
    }
    if order_data.price < 0 {
        return bad_request("price cannot be negative");
    }
    if order_data.margin <= 0 {
        return bad_request("margin must be greater than zero");
    }

    let context = match command_context(&req, user_extension) {
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
    let command = WalletCommand::PlaceOrderIntent(PlaceOrderIntent {
        envelope: context.envelope,
        market_id: order_data.market_id,
        market_name: order_data.market_name,
        side: side_to_protocol(order_data.side),
        order_type: order_type_to_protocol(order_data.order_type),
        quantity: order_data.quantity,
        price: order_data.price,
        margin_asset: asset_to_protocol(order_data.margin_asset),
        required_margin: order_data.margin,
        reduce_only: false,
    });
    let key = user_extension.userid.to_string();
    let receiver = reply_state
        .register_waiter(
            &context.ack.request_id,
            user_extension.userid,
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

#[delete("/")]
pub async fn cancel_order(req: HttpRequest, body: web::Json<CancelOrder>) -> impl Responder {
    let extensions = req.extensions();
    let user_extension = match extensions.get::<Claim>() {
        Some(ue) => ue,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authorized"),
                body: None,
            });
        }
    };

    let order_data = body.into_inner();
    if order_data.market_id <= 0 {
        return bad_request("market_id must be greater than zero");
    }
    if order_data.order_id <= 0 {
        return bad_request("order_id must be greater than zero");
    }

    let context = match command_context(&req, user_extension) {
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
    let command = WalletCommand::CancelOrderIntent(CancelOrderIntent {
        envelope: context.envelope,
        market_id: order_data.market_id,
        order_id: order_data.order_id,
    });
    let key = user_extension.userid.to_string();
    let receiver = reply_state
        .register_waiter(
            &context.ack.request_id,
            user_extension.userid,
            RequestKind::CancelOrder,
        )
        .await;

    if let Err(error) = producer.publish_wallet_command(&key, &command).await {
        eprintln!("{error}");
        reply_state.remove(&context.ack.request_id).await;
        return internal_server_error("failed to queue wallet command");
    }

    final_or_queued_response(receiver, context.wait_timeout, context.ack).await
}

#[get("/{market_id}")]
pub async fn get_all_orders(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
    let extensions = req.extensions();
    let user_extension = match extensions.get::<Claim>() {
        Some(ue) => ue,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authorized"),
                body: None,
            });
        }
    };

    let user_id = user_extension.userid;
    let market_id = path.into_inner();

    match get_users_market_all_orders(user_id, market_id).await {
        Ok(Some(or)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("user orders"),
            body: Some(or),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<OrderRow> {
            success: true,
            info: String::from("empty orders for user"),
            body: None,
        }),
        Err(_) => HttpResponse::InternalServerError().json(ResponseBody::<OrderRow> {
            success: false,
            info: String::from("internal server error"),
            body: None,
        }),
    }
}

#[get("/open/{market_id}")]
pub async fn get_open_orders(_req: HttpRequest, path: web::Path<i64>) -> impl Responder {
    let market_id = path.into_inner();
    match get_all_open_orders(market_id).await {
        Ok(Some(or)) => {
            let orders = or
                .into_iter()
                .map(PublicOpenOrder::from)
                .collect::<Vec<_>>();
            HttpResponse::Ok().json(ResponseBody {
                success: true,
                info: String::from("all open orders"),
                body: Some(orders),
            })
        }
        Ok(None) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("no open orders"),
            body: Some(Vec::<PublicOpenOrder>::new()),
        }),
        Err(_) => HttpResponse::InternalServerError().json(ResponseBody {
            success: false,
            info: String::from("internal server error"),
            body: None::<Vec<PublicOpenOrder>>,
        }),
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::*;

    #[actix_web::test]
    async fn get_all_orders_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_all_orders)).await;
        let req = test::TestRequest::get().uri("/1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn place_order_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(place_order)).await;
        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("content-type", "application/json"))
            .set_payload(
                r#"{
                    "market_id": 1,
                    "market_name": "SOL-PERP",
                    "side": "LONG",
                    "order_type": "LIMIT",
                    "quantity": 10,
                    "price": 100,
                    "margin": 50
                }"#,
            )
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn cancel_order_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(cancel_order)).await;
        let req = test::TestRequest::delete()
            .uri("/")
            .insert_header(("content-type", "application/json"))
            .set_payload(r#"{"market_id": 1, "order_id": 10}"#)
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
