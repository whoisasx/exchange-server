use actix_web::{HttpRequest, HttpResponse, Responder, get, post};

#[post("/")]
pub async fn add_balance(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}

#[get("/")]
pub async fn get_balance(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}

#[get("/{currency}")]
pub async fn get_currency_balance(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}

#[get("/available")]
pub async fn get_available_balance(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}