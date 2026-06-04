use actix_web::{HttpRequest, HttpResponse, Responder, get};

#[get("/open/{market_id}")]
pub async fn get_open_positions(req:HttpRequest)-> impl Responder{
  HttpResponse::Ok()
}

#[get("/close/{market_id}")]
pub async fn get_closed_positions(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}