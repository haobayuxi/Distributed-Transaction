use common::config::Config;
use janus::coordinator::JanusCoordinator;

#[derive(Default)]
struct ConfigPerClient {
    id: i32,
    read_optimize: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let f = std::fs::File::open("config.yml").unwrap();
    // let server_config: ConfigPerServer = serde_yaml::from_reader(f).unwrap();
    // let server = Server::new(0);
    let config = Config::default();
    let client_config = ConfigPerClient::default();
    let mut client = JanusCoordinator::new(client_config.id, client_config.read_optimize, config);
    client.init_rpc().await;
    // client.init_run().await;

    Ok(())
}
