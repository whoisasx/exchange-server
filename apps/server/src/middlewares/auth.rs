use actix_web::{Error, HttpMessage, HttpResponse, body::BoxBody, dev::{ServiceRequest, ServiceResponse}, http::header, middleware::Next, web};
use config::Config;
use jsonwebtoken::{DecodingKey, Validation, decode};

use crate::{modules::auth::dto::Claim, types::ResponseBody};

pub async fn auth_middleware(config: web::Data<Config>, req: ServiceRequest, next: Next<BoxBody>) -> Result<ServiceResponse<BoxBody>, Error>{
  let auth_header=match req.headers().get(header::AUTHORIZATION){
    Some(auth)=> match auth.to_str(){
      Ok(v)=>v,
      Err(_)=>{
          let response=HttpResponse::Unauthorized().json(ResponseBody::<()>{
          success: false,
          info: String::from("invalid header encoding"),
          body: None
        });
        return Ok(req.into_response(response));
      }
    }
    None=>{
      let response=HttpResponse::Unauthorized().json(ResponseBody::<()>{
        success: false,
        info: String::from("authorization header is missing"),
        body: None
      });
      return Ok(req.into_response(response));
    }
  };

  if !auth_header.starts_with("Bearer ") {
    let response=HttpResponse::Unauthorized().json(ResponseBody::<()>{
      success: false,
      info: String::from("authorization header must contain 'Bearer ' "),
      body: None
    });
    return Ok(req.into_response(response));
  }

  let auth_token=&auth_header[7..];

  let payload=match decode::<Claim>(&auth_token, &DecodingKey::from_secret(config.jwt_secret.as_ref()), &Validation::new(jsonwebtoken::Algorithm::HS256)){
    Ok(pl)=>pl.claims,
    Err(_)=>{
      let response=HttpResponse::Unauthorized().json(ResponseBody::<()>{
        success: false,
        info: String::from("invalid token"),
        body: None
      });
      return Ok(req.into_response(response));
    }
  };

  req.extensions_mut().insert(payload);

  next.call(req).await
}