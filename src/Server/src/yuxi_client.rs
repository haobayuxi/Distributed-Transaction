use std::env;

use common::{config::Config, ConfigInFile};
use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc;
use yuxi::coordinator::YuxiCoordinator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let client_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();

    let config = Config::default();
    // let client_config = ConfigPerClient::default();
    let (sender, receiver) = mpsc::channel::<YuxiMsg>(1000);
    let mut client = YuxiCoordinator::new(
        id,
        config,
        client_config.read_perc,
        client_config.txns_per_client,
        receiver,
    );
    client.init_run(sender).await;
    Ok(())
}
