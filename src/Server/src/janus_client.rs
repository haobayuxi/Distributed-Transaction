use std::env;

use common::{config::Config, ConfigInFile};
use janus::coordinator::JanusCoordinator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<i32>().unwrap();
    let config = Config::default();
    let client_config = ConfigInFile::default();
    let mut client = JanusCoordinator::new(
        id,
        config,
        client_config.txns_per_client,
        client_config.read_perc,
    );
    client.init_run().await;
    // client.init_run().await;

    Ok(())
}
