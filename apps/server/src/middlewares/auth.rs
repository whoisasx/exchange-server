use actix_web::{Error, body::BoxBody, dev::{ServiceRequest, ServiceResponse}, middleware::Next};

pub async fn auth_middleware(req: ServiceRequest, next: Next<BoxBody>) -> Result<ServiceResponse<BoxBody>, Error>{
  next.call(req).await
}