use serde::Serialize;

#[derive(Serialize)]
pub struct ResponseBody<T> {
    pub success: bool,
    pub info: String,
    pub body: Option<T>,
}
