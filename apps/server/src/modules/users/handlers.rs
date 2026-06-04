use actix_web::{HttpRequest, HttpResponse, Responder, get};

#[get("/")]
pub async fn get_all_users(req: HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}
#[get("/me")]
pub async fn get_user_details(req: HttpRequest) -> impl  Responder{
  HttpResponse::Ok()
}