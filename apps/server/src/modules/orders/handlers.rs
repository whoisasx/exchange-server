use actix_web::{HttpRequest, HttpResponse, Responder, delete, get, post};

#[post("/")]
pub async fn place_order(req:HttpRequest)->impl Responder{
  HttpResponse::Ok()
}

#[delete("/")]
pub async fn cancel_order(req:HttpRequest)-> impl Responder{
  HttpResponse::Ok()
}

#[get("/{market_id}")]
pub async  fn get_all_orders(req:HttpRequest) -> impl Responder{
  HttpResponse::Ok()
}

#[get("/open/{market_id}")]
pub async fn get_open_orders(req:HttpRequest) -> impl Responder {
  HttpResponse::Ok()
}