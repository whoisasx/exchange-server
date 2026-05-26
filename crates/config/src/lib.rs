use dotenvy::dotenv;

pub fn config_envs() -> Result<(), dotenvy::Error> {
    dotenv().map(|_| ())
}
