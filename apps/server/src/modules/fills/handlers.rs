use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, web};
use db::dto::FillRow;

use crate::{
    modules::{
        auth::dto::Claim,
        fills::services::{
            FillServiceError, get_order_id_fills, get_position_closed_id_fills,
            get_position_id_fills, get_user_fills,
        },
    },
    utils::types::ResponseBody,
};

#[get("/")]
pub async fn get_fills(req: HttpRequest) -> impl Responder {
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

    match get_user_fills(user_extension.userid).await {
        Ok(Some(fills)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("fills fetched"),
            body: Some(fills),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<FillRow> {
            success: true,
            info: String::from("no fills"),
            body: None,
        }),
        Err(FillServiceError::Forbidden) => {
            HttpResponse::Forbidden().json(ResponseBody::<Vec<FillRow>> {
                success: false,
                info: String::from("user does not own this resource"),
                body: None,
            })
        }
        Err(FillServiceError::Storage) => {
            HttpResponse::InternalServerError().json(ResponseBody::<FillRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

#[get("/orders/{order_id}")]
pub async fn get_orders_fills(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
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

    let order_id = path.into_inner();
    match get_order_id_fills(user_extension.userid, order_id).await {
        Ok(Some(fills)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("fills fetched"),
            body: Some(fills),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<FillRow> {
            success: true,
            info: String::from("no fills"),
            body: None,
        }),
        Err(FillServiceError::Forbidden) => {
            HttpResponse::Forbidden().json(ResponseBody::<Vec<FillRow>> {
                success: false,
                info: String::from("user does not own this resource"),
                body: None,
            })
        }
        Err(FillServiceError::Storage) => {
            HttpResponse::InternalServerError().json(ResponseBody::<FillRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

pub async fn get_positions_fills(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
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

    let position_id = path.into_inner();
    match get_position_id_fills(user_extension.userid, position_id).await {
        Ok(Some(fills)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("fills fetched"),
            body: Some(fills),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<FillRow> {
            success: true,
            info: String::from("no fills"),
            body: None,
        }),
        Err(FillServiceError::Forbidden) => {
            HttpResponse::Forbidden().json(ResponseBody::<Vec<FillRow>> {
                success: false,
                info: String::from("user does not own this resource"),
                body: None,
            })
        }
        Err(FillServiceError::Storage) => {
            HttpResponse::InternalServerError().json(ResponseBody::<FillRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

#[get("/closed-positions/{position_id}")]
pub async fn get_closed_positions_fills(req: HttpRequest, path: web::Path<i64>) -> impl Responder {
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

    let closed_position_id = path.into_inner();
    match get_position_closed_id_fills(user_extension.userid, closed_position_id).await {
        Ok(Some(fills)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("fills fetched"),
            body: Some(fills),
        }),
        Ok(None) => HttpResponse::Ok().json(ResponseBody::<FillRow> {
            success: true,
            info: String::from("no fills"),
            body: None,
        }),
        Err(FillServiceError::Forbidden) => {
            HttpResponse::Forbidden().json(ResponseBody::<Vec<FillRow>> {
                success: false,
                info: String::from("user does not own this resource"),
                body: None,
            })
        }
        Err(FillServiceError::Storage) => {
            HttpResponse::InternalServerError().json(ResponseBody::<FillRow> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};

    use super::*;

    #[actix_web::test]
    async fn get_fills_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_fills)).await;
        let req = test::TestRequest::get().uri("/").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_orders_fills_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_orders_fills)).await;
        let req = test::TestRequest::get().uri("/orders/1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_positions_fills_rejects_requests_without_claim() {
        let app = test::init_service(App::new().route(
            "/positions/{position_id}",
            web::get().to(get_positions_fills),
        ))
        .await;
        let req = test::TestRequest::get().uri("/positions/1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_closed_positions_fills_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_closed_positions_fills)).await;
        let req = test::TestRequest::get()
            .uri("/closed-positions/1")
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
