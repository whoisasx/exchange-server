use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, post, web};
use db::dto::{AssetType, UserCollateralRow};

use crate::{
    modules::{
        auth::dto::Claim,
        balances::services::{get_user_asset_balances, get_user_balances},
    },
    utils::types::ResponseBody,
};

#[post("/")]
pub async fn add_balance(req: HttpRequest) -> impl Responder {
    let extension = req.extensions();
    let _user_extension = match extension.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    HttpResponse::Ok().body("adil")
    //TODO: hot-path
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
pub async fn withdraw_balance(req: HttpRequest) -> impl Responder {
    let extension = req.extensions();
    let _user_extension = match extension.get::<Claim>() {
        Some(ex) => ex,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    //TODO: hot path
    HttpResponse::Ok().body("hi")
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
}
