use common::{config::Config, ConfigInFile};

use dast::{peer::Peer, Msg};

use tokio::sync::mpsc::unbounded_channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    println!("server id = {}", server_config.id);
    let config = Config::default();
    let (sender, recv) = unbounded_channel::<Msg>();
    let mut peer = Peer::new(server_config.id, 3, config, recv, server_config.is_ycsb);
    peer.init_run(sender).await;
    Ok(())
}
