use std::env;
use dotenvy::dotenv;

#[derive(Debug)]
pub struct Config{
    pub database_url: String
}

impl Config{
    pub fn init() -> Config {
        dotenv().ok();
        let database_url=env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        Config { 
            database_url
        }
    }
}

