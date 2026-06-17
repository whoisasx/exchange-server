use actix_web::{HttpMessage, HttpRequest, HttpResponse, Responder, get, web};

use crate::{modules::auth::dto::Claim, replies::ReplyState, utils::types::ResponseBody};

#[get("/{request_id}")]
pub async fn get_request_status(req: HttpRequest, path: web::Path<String>) -> impl Responder {
    let extensions = req.extensions();
    let claim = match extensions.get::<Claim>() {
        Some(claim) => claim,
        None => {
            return HttpResponse::Unauthorized().json(ResponseBody::<Claim> {
                success: false,
                info: String::from("user not authenticated."),
                body: None,
            });
        }
    };

    let Some(reply_state) = req.app_data::<web::Data<ReplyState>>() else {
        return HttpResponse::InternalServerError().json(ResponseBody::<()> {
            success: false,
            info: String::from("reply state is not initialized"),
            body: None,
        });
    };

    let request_id = path.into_inner();
    match reply_state.get_for_user(&request_id, claim.userid).await {
        Some(record) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("request status"),
            body: Some(record),
        }),
        None => HttpResponse::NotFound().json(ResponseBody::<()> {
            success: false,
            info: String::from("request not found"),
            body: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::*;

    #[actix_web::test]
    async fn get_request_status_rejects_requests_without_claim() {
        let app = test::init_service(App::new().service(get_request_status)).await;
        let req = test::TestRequest::get().uri("/req-1").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
