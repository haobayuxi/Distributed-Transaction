use std::env;

use common::{config::Config, ConfigInFile};
use meerkat::{coordinator::MeerkatCoordinator, MeerkatMeta};
use rpc::meerkat::MeerkatMsg;
use tokio::sync::mpsc::channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    // let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let client_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();

    let config = Config::default();
    let (result_sender, mut recv) = channel::<(f64)>(10000);
    for i in 0..client_config.client_num {
        let c = config.clone();
        let sender = result_sender.clone();
        tokio::spawn(async move {
            let (client_msg_sender, recv) = channel::<MeerkatMsg>(1000);
            let mut client = MeerkatCoordinator::new(
                i,
                c,
                client_config.read_perc,
                client_config.txns_per_client,
                recv,
                client_config.zipf,
            );
            sender.send(client.init_run(client_msg_sender).await).await;
        });
    }
    let mut final_thoughput = 0.0;
    for i in 0..60 {
        final_thoughput += recv.recv().await.unwrap();
    }
    println!("throughput = {}", final_thoughput);

    Ok(())
}
