use actix_web::{HttpRequest, HttpResponse, Responder, post};

#[post("/signup")]
pub async fn signup_user(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}

#[post("/signin")]
pub async fn signin_user(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}