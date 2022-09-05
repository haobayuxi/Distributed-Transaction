use std::{env, time::Duration};

use common::{config::Config, ycsb::YcsbQuery, ConfigInFile};
use rpc::{
    common::{TxnOp, TxnType},
    dast::{client_service_client::ClientServiceClient, dast_client::DastClient, DastMsg},
};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    time::{sleep, Instant},
};
use tonic::transport::Channel;

pub struct ProposeClient {
    id: i32,
    client: ClientServiceClient<Channel>,
    is_ycsb: bool,
    txn_id: i64,
    txn: DastMsg,
    workload: YcsbQuery,
    txns_per_client: i32,
}

impl ProposeClient {
    pub async fn new(
        config: Config,
        read_perc: i32,
        id: i32,
        txns_per_client: i32,
        is_ycsb: bool,
    ) -> Self {
        let server_id = id % 3;
        let addr_to_connect = config.server_addrs.get(&server_id).unwrap().clone();
        loop {
            match ClientServiceClient::connect(addr_to_connect.clone()).await {
                Ok(client) => {
                    return Self {
                        client,
                        id,
                        is_ycsb,
                        txn_id: (id as i64) << 45,
                        txn: DastMsg::default(),
                        workload: YcsbQuery::new(
                            config.zipf_theta,
                            config.req_per_query as i32,
                            read_perc,
                        ),
                        txns_per_client,
                    };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn run_transaction(&mut self) {
        let mut latency_result = Vec::new();
        // send msgs
        let total_start = Instant::now();
        for i in 0..self.txns_per_client {
            self.workload.generate();
            self.txn_id += 1;
            self.txn = DastMsg {
                txn_id: self.txn_id,
                read_set: self.workload.read_set.clone(),
                write_set: self.workload.write_set.clone(),
                notified_txn_ts: Vec::new(),
                op: TxnOp::Prepare.into(),
                from: self.id,
                timestamp: 0,
                maxts: 0,
                txn_type: Some(TxnType::Ycsb.into()),
            };
            let start = Instant::now();
            let _reply = self.client.propose(self.txn.clone()).await;
            let end_time = start.elapsed().as_micros();
            latency_result.push(end_time);
        }
        let total_end = (total_start.elapsed().as_millis() as f64) / 1000.0;
        let throughput_result = self.txns_per_client as f64 / total_end;
        println!("throughput = {}", throughput_result);
        // write results to file
        let latency_file_name = self.id.to_string() + "latency.data";
        let mut latency_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(latency_file_name)
            .await
            .unwrap();
        for iter in latency_result {
            latency_file.write(iter.to_string().as_bytes()).await;
            latency_file.write("\n".as_bytes()).await;
        }
        let throughput_file_name = self.id.to_string() + "throughput.data";
        let mut throughput_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(throughput_file_name)
            .await
            .unwrap();
        throughput_file
            .write(throughput_result.to_string().as_bytes())
            .await;
        throughput_file.write("\n".as_bytes()).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<i32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let config_in_file: ConfigInFile = serde_yaml::from_reader(f).unwrap();

    let config = Config::default();
    // let client_config = ConfigPerClient::default();
    let mut client = ProposeClient::new(
        config,
        config_in_file.read_perc,
        id,
        config_in_file.txns_per_client,
        config_in_file.is_ycsb,
    )
    .await;
    client.run_transaction().await;
    Ok(())
}
