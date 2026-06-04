use actix_web::{HttpRequest, HttpResponse, Responder, get};

#[get("/")]
pub async fn get_fills(req:HttpRequest)-> impl Responder{
  HttpResponse::Ok()
}