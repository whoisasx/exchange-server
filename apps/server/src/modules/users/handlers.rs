use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get};

use crate::{
    modules::{
        auth::dto::Claim,
        users::{
            dto::UserProfile,
            services::{get_user_by_userid, list_users},
        },
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

#[get("/")]
pub async fn get_all_users(req: HttpRequest) -> impl Responder {
    if let Err(response) = authenticated_user(&req) {
        return response;
    }

    match list_users().await {
        Ok(users) => {
            let profiles = users.into_iter().map(UserProfile::from).collect::<Vec<_>>();
            HttpResponse::Ok().json(ResponseBody {
                success: true,
                info: String::from("users fetched"),
                body: Some(profiles),
            })
        }
        Err(_) => HttpResponse::InternalServerError().json(ResponseBody::<Vec<UserProfile>> {
            success: false,
            info: String::from("internal server error"),
            body: None,
        }),
    }
}

#[get("/me")]
pub async fn get_user_details(req: HttpRequest) -> impl Responder {
    let claim = match authenticated_user(&req) {
        Ok(claim) => claim,
        Err(response) => return response,
    };

    match get_user_by_userid(claim.userid).await {
        Ok(Some(user)) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("user details"),
            body: Some(UserProfile::from(user)),
        }),
        Ok(None) => HttpResponse::NotFound().json(ResponseBody::<UserProfile> {
            success: false,
            info: String::from("user does not exist"),
            body: None,
        }),
        Err(_) => HttpResponse::InternalServerError().json(ResponseBody::<UserProfile> {
            success: false,
            info: String::from("internal server error"),
            body: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::*;

    #[actix_web::test]
    async fn get_all_users_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_all_users)).await;
        let req = test::TestRequest::get().uri("/").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn get_user_details_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_user_details)).await;
        let req = test::TestRequest::get().uri("/me").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
