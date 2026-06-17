use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, post, web};
use db::dto::{AssetType, UserCollateralRow};
use protocol::wallet::{Deposit, WalletCommand, Withdraw};

use crate::{
    hot_path::{
        bad_request, command_context, final_or_queued_response, internal_server_error,
        redpanda_producer, reply_state,
    },
    modules::{
        auth::dto::Claim,
        balances::{
            dto::{DepositBalance, WithdrawBalance},
            services::{get_user_asset_balances, get_user_balances},
        },
    },
    protocol_map::asset_to_protocol,
    replies::RequestKind,
    utils::types::ResponseBody,
};

#[post("/")]
pub async fn add_balance(req: HttpRequest, body: web::Json<DepositBalance>) -> impl Responder {
    let extension = req.extensions();
    let user_extension = match extension.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    let deposit = body.into_inner();
    if deposit.amount <= 0 {
        return bad_request("amount must be greater than zero");
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
    let command = WalletCommand::Deposit(Deposit {
        envelope: context.envelope,
        asset: asset_to_protocol(deposit.asset),
        amount: deposit.amount,
        reference_id: deposit
            .reference_id
            .unwrap_or_else(|| context.ack.idempotency_key.clone()),
    });
    let key = user_extension.userid.to_string();
    let receiver = reply_state
        .register_waiter(
            &context.ack.request_id,
            user_extension.userid,
            RequestKind::Deposit,
        )
        .await;

    if let Err(error) = producer.publish_wallet_command(&key, &command).await {
        eprintln!("{error}");
        reply_state.remove(&context.ack.request_id).await;
        return internal_server_error("failed to queue wallet command");
    }

    final_or_queued_response(receiver, context.wait_timeout, context.ack).await
}

#[get("/")]
pub async fn get_balance(req: HttpRequest) -> impl Responder {
    let extension = req.extensions();
    let user_extension = match extension.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    let user_id = user_extension.userid;
    let user_balances = match get_user_balances(user_id).await {
        Ok(Some(ub)) => ub,
        Ok(None) => {
            return HttpResponse::Ok().json(ResponseBody {
                success: true,
                info: String::from("user's wallet is empty"),
                body: Some(Vec::<UserCollateralRow>::new()),
            });
        }
        Err(_) => {
            return HttpResponse::InternalServerError().json(ResponseBody::<UserCollateralRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            });
        }
    };

    HttpResponse::Ok().json(ResponseBody {
        success: true,
        info: String::from("user's balances"),
        body: Some(user_balances),
    })
}

#[get("/{currency}")]
pub async fn get_currency_balance(req: HttpRequest, path: web::Path<AssetType>) -> impl Responder {
    let currency_type = path.into_inner();
    let extensions = req.extensions();
    let user_extension = match extensions.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    let asset_balance = match get_user_asset_balances(user_extension.userid, currency_type).await {
        Ok(Some(ab)) => ab,
        Ok(None) => {
            return HttpResponse::Ok().json(ResponseBody::<UserCollateralRow> {
                success: true,
                info: String::from("user has null asset value"),
                body: None,
            });
        }
        Err(_) => {
            return HttpResponse::InternalServerError().json(ResponseBody::<UserCollateralRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            });
        }
    };

    HttpResponse::Ok().json(ResponseBody {
        success: true,
        info: String::from("asset values"),
        body: Some(asset_balance),
    })
}

#[post("/withdraw")]
pub async fn withdraw_balance(
    req: HttpRequest,
    body: web::Json<WithdrawBalance>,
) -> impl Responder {
    let extension = req.extensions();
    let user_extension = match extension.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    let withdraw = body.into_inner();
    if withdraw.amount <= 0 {
        return bad_request("amount must be greater than zero");
    }
    if withdraw.destination.trim().is_empty() {
        return bad_request("destination cannot be empty");
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
    let command = WalletCommand::Withdraw(Withdraw {
        envelope: context.envelope,
        asset: asset_to_protocol(withdraw.asset),
        amount: withdraw.amount,
        destination: withdraw.destination,
    });
    let key = user_extension.userid.to_string();
    let receiver = reply_state
        .register_waiter(
            &context.ack.request_id,
            user_extension.userid,
            RequestKind::Withdraw,
        )
        .await;

    if let Err(error) = producer.publish_wallet_command(&key, &command).await {
        eprintln!("{error}");
        reply_state.remove(&context.ack.request_id).await;
        return internal_server_error("failed to queue wallet command");
    }

    final_or_queued_response(receiver, context.wait_timeout, context.ack).await
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::*;

    #[actix_web::test]
    async fn get_balance_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_balance)).await;
        let req = test::TestRequest::get().uri("/").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_currency_balance_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_currency_balance)).await;
        let req = test::TestRequest::get().uri("/USDC").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn add_balance_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(add_balance)).await;
        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("content-type", "application/json"))
            .set_payload(r#"{"asset":"USDC","amount":100}"#)
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn withdraw_balance_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(withdraw_balance)).await;
        let req = test::TestRequest::post()
            .uri("/withdraw")
            .insert_header(("content-type", "application/json"))
            .set_payload(r#"{"asset":"USDC","amount":100,"destination":"bank-1"}"#)
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
