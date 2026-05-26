use dotenv::dotenv;

pub fn config_envs() -> Result<(), dotenv::Error> {
    dotenv().map(|_| ())
}
