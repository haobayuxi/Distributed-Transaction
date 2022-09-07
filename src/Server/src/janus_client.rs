use std::env;

use common::{config::Config, ConfigInFile};
use janus::coordinator::JanusCoordinator;
use rpc::janus::JanusMsg;
use tokio::sync::mpsc::channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let config = Config::default();
    let f = std::fs::File::open("config.yml").unwrap();
    let client_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let (sender, recv) = channel::<JanusMsg>(100000);
    let mut client = JanusCoordinator::new(
        id,
        config,
        client_config.txns_per_client,
        client_config.read_perc,
        recv,
    );
    client.init_run(sender).await;
    // client.init_run().await;

    Ok(())
}
