use actix_web::{ HttpResponse, Responder, post, web::{self}};
use bcrypt::{DEFAULT_COST, hash, verify};
use config::Config;
use jsonwebtoken::{EncodingKey, Header, encode};
use rand::RngExt;

use crate::{modules::auth::{dto::{AuthUser, Claim, UserRecord}, services::{get_user_by_username, is_user_exist, register_user}}, types::ResponseBody};

fn generate_user_id()->u64{
  rand::rng().random()
}

#[post("/signup")]
pub async fn signup_user(body: web::Json<AuthUser>, config:web::Data<Config>) -> impl Responder{
  let user_info=body.into_inner();

  match is_user_exist(&user_info.username).await {
    Ok(is_exist)=>{
      if is_exist {
        return HttpResponse::BadRequest().json(ResponseBody{
          success: false,
          info: String::from("user already exists."),
          body:None::<UserRecord>
        })
      }
    },
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error."),
        body:None::<UserRecord>
      })
    }
  };

  let user_id=generate_user_id();

  let hashed_bytes_pass=match hash(&user_info.password, DEFAULT_COST){
    Ok(h)=>h,
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error."),
        body:None::<UserRecord>
      })
    }
  };
  let hashed_password=hex::encode(hashed_bytes_pass);

  match register_user(&user_id.to_string(), &user_info.username, &hashed_password, &user_id.to_string()).await {
    Ok(_)=>{},
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error."),
        body:None::<UserRecord>
      })
    }
  };

  let my_claims=Claim{
    userid: user_id,
    username:user_info.username.clone()
  };
  let jwt_token= match encode(&Header::default(), &my_claims, &EncodingKey::from_secret(&config.jwt_secret.as_ref())) {
    Ok(token)=>token,
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error."),
        body:None::<UserRecord>
      })
    }
  };

  let user_record=UserRecord{
    username: user_info.username,
    userid: user_id,
    jwt_token
  };
  HttpResponse::Created().json(ResponseBody{
    success: true,
    info: String::from("user registered"),
    body: Some(user_record)
  })
}

#[post("/signin")]
pub async fn signin_user(body:web::Json<AuthUser>, config: web::Data<Config>) -> impl Responder{
  let user_info=body.into_inner();

  let user_details= match get_user_by_username(&user_info.username).await {
    Ok(Some(u))=>u,
    Ok(None)=>{
      return HttpResponse::NotFound().json(ResponseBody{
        success: false,
        info: String::from("user does not exist"),
        body:None::<UserRecord>
      })
    }
    Err(()) =>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error"),
        body:None::<UserRecord>
      })
    }
  };

  match verify(user_info.password, &user_details.hashed_password){
    Ok(flag)=>{
      if !flag {
        return HttpResponse::Unauthorized().json(ResponseBody{
          success: false,
          info: String::from("password incorrect"),
          body:None::<UserRecord>
        })
      }
    },
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error"),
        body:None::<UserRecord>
      })
    }
  }

  let user_id=match user_details.user_id.parse::<u64>(){
    Ok(u)=>u,
    Err(_)=>{
      return HttpResponse::BadRequest().json(ResponseBody{
        success: false,
        info: String::from("invalid user id format"),
        body:None::<UserRecord>
      })
    }
  };

  let my_claims=Claim{
    userid: user_id,
    username: user_details.username.clone()
  };
  let jwt_token=match encode(&Header::default(), &my_claims, &EncodingKey::from_secret(&config.jwt_secret.as_ref())) {
    Ok(j)=>j,
    Err(_)=>{
      return HttpResponse::InternalServerError().json(ResponseBody{
        success: false,
        info: String::from("internal server error"),
        body:None::<UserRecord>
      })
    }
  };

  HttpResponse::Ok().json(ResponseBody{
    success: true,
    info: String::from("user logged in"),
    body: Some(UserRecord{
      userid: user_id,
      username: user_details.username,
      jwt_token
    })
  })
}