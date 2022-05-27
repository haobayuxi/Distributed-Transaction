use std::env;

use common::{config::Config, ConfigPerClient};
use tapir::coordinator::TapirCoordinator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<i32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let client_config: ConfigPerClient = serde_yaml::from_reader(f).unwrap();

    let config = Config::default();
    // let client_config = ConfigPerClient::default();
    let mut client = TapirCoordinator::new(
        client_config.id,
        client_config.read_optimize,
        config,
        client_config.read_perc,
    );
    client.init_run().await;
    Ok(())
}
