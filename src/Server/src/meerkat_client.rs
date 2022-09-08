use std::env;

use common::{config::Config, ConfigInFile};
use meerkat::{coordinator::MeerkatCoordinator, MeerkatMeta};
use rpc::meerkat::MeerkatMsg;
use tokio::sync::mpsc::channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let client_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();

    let config = Config::default();
    // let client_config = ConfigPerClient::default();
    let (sender, recv) = channel::<MeerkatMsg>(1000);
    let mut client = MeerkatCoordinator::new(
        id,
        config,
        client_config.read_perc,
        client_config.txns_per_client,
        recv,
        client_config.zipf,
    );
    client.init_run(sender).await;
    Ok(())
}
