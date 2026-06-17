use std::error::Error;

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, error, get, web};
use ws::{
    auth::authenticate_request, consumer::EventConsumers, hub::Hub, router::EventRouter,
    session::WsSession, settings::WsSettings,
};

#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let settings = WsSettings::from_env();
    let hub = Hub::default();
    let router = EventRouter::new(hub.clone());
    let consumers = EventConsumers::new(&settings, router).await?;
    consumers.spawn();

    let host = settings.ws_host.clone();
    let port = settings.ws_port;
    let hub_data = web::Data::new(hub);
    let settings_data = web::Data::new(settings);

    println!("ws listening on {host}:{port}");

    HttpServer::new(move || {
        App::new()
            .app_data(hub_data.clone())
            .app_data(settings_data.clone())
            .service(connect_ws)
            .service(health)
    })
    .shutdown_timeout(30)
    .bind((host, port))?
    .run()
    .await?;

    Ok(())
}

#[get("/ws")]
async fn connect_ws(
    req: HttpRequest,
    stream: web::Payload,
    hub: web::Data<Hub>,
    settings: web::Data<WsSettings>,
) -> Result<HttpResponse, actix_web::Error> {
    let claim = authenticate_request(&req, &settings.jwt_secret)
        .map_err(|error| error::ErrorUnauthorized(error.to_string()))?;
    let session = WsSession::new(claim, hub.get_ref().clone());

    actix_web_actors::ws::start(session, &req, stream)
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().body("ok")
}
