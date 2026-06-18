use actix_web::{HttpResponse, Responder, get, web};
use db::dto::CandleRow;
use serde::Deserialize;

use crate::{
    modules::markets::services::{MarketServiceError, get_candles},
    utils::types::ResponseBody,
};

#[derive(Debug, Deserialize)]
pub struct CandleQuery {
    interval: String,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    limit: Option<i64>,
}

#[get("/{market_id}/candles")]
pub async fn get_market_candles(
    path: web::Path<i64>,
    query: web::Query<CandleQuery>,
) -> impl Responder {
    let market_id = path.into_inner();
    if market_id <= 0 {
        return bad_request("market_id must be greater than zero");
    }

    match get_candles(
        market_id,
        &query.interval,
        query.start_ms,
        query.end_ms,
        query.limit,
    )
    .await
    {
        Ok(candles) => HttpResponse::Ok().json(ResponseBody {
            success: true,
            info: String::from("candles fetched"),
            body: Some(candles),
        }),
        Err(MarketServiceError::InvalidInterval) => bad_request("unsupported candle interval"),
        Err(MarketServiceError::InvalidLimit) => bad_request("limit must be between 1 and 1000"),
        Err(MarketServiceError::InvalidTimestamp) => {
            bad_request("start_ms and end_ms must be valid unix milliseconds")
        }
        Err(MarketServiceError::InvalidTimeRange) => {
            bad_request("start_ms must be less than end_ms")
        }
        Err(MarketServiceError::Storage) => {
            HttpResponse::InternalServerError().json(ResponseBody::<Vec<CandleRow>> {
                success: false,
                info: String::from("internal server error"),
                body: None,
            })
        }
    }
}

fn bad_request(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(ResponseBody::<Vec<CandleRow>> {
        success: false,
        info: String::from(message),
        body: None,
    })
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::*;

    #[actix_web::test]
    async fn get_market_candles_rejects_invalid_interval() {
        let app = test::init_service(App::new().service(get_market_candles)).await;
        let req = test::TestRequest::get()
            .uri("/1/candles?interval=2m")
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn get_market_candles_rejects_invalid_limit() {
        let app = test::init_service(App::new().service(get_market_candles)).await;
        let req = test::TestRequest::get()
            .uri("/1/candles?interval=1m&limit=1001")
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
