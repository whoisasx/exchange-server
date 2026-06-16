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
pub async fn close_position(_req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}

#[post("/liquidate")]
pub async fn liquidate_position(_req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}
