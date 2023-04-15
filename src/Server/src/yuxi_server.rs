use common::{config::Config, ConfigInFile};

use tokio::sync::mpsc::unbounded_channel;
use yuxi::peer::Peer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    println!("server id = {}", server_config.id);
    let config = Config::default();
    let mut peer = Peer::new(server_config.id, config);
    peer.init().await;
    Ok(())
}
