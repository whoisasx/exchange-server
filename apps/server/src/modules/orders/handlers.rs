use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, delete, get, post, web};
use db::dto::OrderRow;

use crate::{
    modules::{
        auth::dto::Claim,
        orders::{
            dto::{CancelOrder, PlaceOrder, PublicOpenOrder},
            services::{get_all_open_orders, get_users_market_all_orders},
        },
    },
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

    let _user_id = user_extension.userid;
    let _order_data = body.into_inner();
    //TODO: HOT-PATH
    HttpResponse::Ok().body("hi")
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

    let _user_id = user_extension.userid;
    let _order_data = body.into_inner();
    //TODO: HOT-PATH
    HttpResponse::Ok().body("hi")
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
