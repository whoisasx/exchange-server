use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub server_url: String,
    pub server_port: u16,
    pub server_host: String,
    pub jwt_secret: String,
}

impl Config {
    pub fn init() -> Config {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let server_url = env::var("SERVER_URL").expect("SERVER_URL must be set");
        let server_port = env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .expect("SERVER_PORT must be set");
        let server_host = env::var("SERVER_HOST").expect("SERVER_HOST must be set");
        let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");

        Config {
            database_url,
            server_url,
            server_port,
            server_host,
            jwt_secret,
        }
    }
}
