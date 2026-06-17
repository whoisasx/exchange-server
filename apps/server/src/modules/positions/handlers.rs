use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, post, web};
use db::dto::{ClosedPositionRow, PositionRow};

use crate::{
    modules::{
        auth::dto::Claim,
        positions::services::{get_user_closed_positions, get_user_open_position},
    },
    utils::types::ResponseBody,
};

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
pub async fn close_position(req: HttpRequest) -> impl Responder {
    if let Err(response) = authenticated_user(&req) {
        return response;
    }

    HttpResponse::NotImplemented().json(ResponseBody::<()> {
        success: false,
        info: String::from("close position command is not implemented yet"),
        body: None,
    })
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
        let req = test::TestRequest::post().uri("/close").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
