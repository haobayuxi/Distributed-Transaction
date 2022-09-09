use chrono::Local;
use std::{collections::HashMap, time::Duration};

use common::{config::Config, ycsb::YcsbQuery, CID_LEN};
use rpc::{common::TxnOp, yuxi::YuxiMsg};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{channel, unbounded_channel, Receiver, Sender},
    time::{sleep, Instant, Sleep},
};
use tonic::transport::Channel;

use crate::peer_communication::RpcClient;

static RETRY: i32 = 20;

pub struct YuxiCoordinator {
    config: Config,
    is_ycsb: bool,
    id: u32,
    txn_id: u64,
    // sharded txn
    txn: YuxiMsg,
    // send to servers
    servers: HashMap<u32, Sender<YuxiMsg>>,
    recv: Receiver<YuxiMsg>,
    workload: YcsbQuery,
    txns_per_client: i32,
}

impl YuxiCoordinator {
    pub fn new(
        id: u32,
        config: Config,
        read_perc: i32,
        txns_per_client: i32,
        recv: Receiver<YuxiMsg>,
        zipf_theta: f64,
    ) -> Self {
        Self {
            is_ycsb: true,
            id,
            txn_id: (id as u64) << CID_LEN,
            txn: YuxiMsg::default(),
            servers: HashMap::new(),
            recv,
            workload: YcsbQuery::new(zipf_theta, config.req_per_query as i32, read_perc),
            config,
            txns_per_client,
        }
    }

    pub async fn init_run(&mut self, sender: Sender<YuxiMsg>) {
        // self.init_workload();
        self.init_rpc(sender).await;
        println!("init rpc done");
        // run transactions
        let mut latency_result = Vec::new();
        self.txn.from = self.id;
        println!("start exec txns {}", self.txns_per_client);
        let total_start = Instant::now();

        for i in 0..self.txns_per_client {
            self.workload.generate();
            println!("{}", i);
            self.txn.txn_id = self.txn_id;
            self.txn.read_set = self.workload.read_set.clone();
            self.txn.write_set = self.workload.write_set.clone();
            let start = Instant::now();
            if self.workload.read_only {
                self.txn.op = TxnOp::ReadOnly.into();
                self.run_readonly().await;
            } else {
                self.txn.op = TxnOp::Prepare.into();
                self.run_transaction().await;
            }

            let end_time = start.elapsed().as_micros();
            println!("latency time = {}", end_time);
            latency_result.push(end_time);

            self.txn_id += 1;
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
        // sleep(Duration::from_secs(100)).await;
    }

    async fn run_readonly(&mut self) {
        let server_id = self.id % 3;
        // let server_id = ;
        println!("read only");
        let time = (Local::now().timestamp_nanos() / 1000) as u64;
        self.txn.timestamp = time;
        self.servers
            .get(&server_id)
            .unwrap()
            .send(self.txn.clone())
            .await
            .unwrap();
        let _res = self.recv.recv().await.unwrap();
    }

    async fn run_transaction(&mut self) -> bool {
        // init ts
        println!("txn");
        let timestamp = (Local::now().timestamp_nanos() / 1000) as u64;

        // prepare, prepare will send to all the server
        self.txn.timestamp = timestamp;

        self.broadcast(self.txn.clone()).await;

        // handle prepare response
        let mut final_ts_vec = Vec::new();
        for i in 0..3 {
            let prepare_res = self.recv.recv().await.unwrap();
            final_ts_vec.push(prepare_res.timestamp);
        }
        final_ts_vec.sort();
        let commit = if final_ts_vec[2] == final_ts_vec[1] {
            true
        } else {
            false
        };
        if !commit {
            // accept
            let accept = YuxiMsg {
                txn_id: self.txn_id,
                read_set: Vec::new(),
                write_set: Vec::new(),
                op: TxnOp::Accept.into(),
                from: self.id,
                timestamp: final_ts_vec[2],
                txn_type: self.txn.txn_type,
            };
            self.broadcast(accept).await;
            for i in 0..3 {
                let accept_res = self.recv.recv().await.unwrap();
            }
        }
        // broadcast commit

        let commit = YuxiMsg {
            txn_id: self.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::Commit.into(),
            from: self.id,
            timestamp: final_ts_vec[2],
            txn_type: self.txn.txn_type,
        };
        // txn success
        self.broadcast(commit).await;
        // wait for result
        self.recv.recv().await;
        return true;
    }

    async fn broadcast(&mut self, msg: YuxiMsg) {
        for (id, server) in self.servers.iter() {
            server.send(msg.clone()).await;
        }
    }

    pub async fn init_rpc(&mut self, sender: Sender<YuxiMsg>) {
        // init rpc client to connect to other peers
        for (id, ip) in self.config.server_addrs.iter() {
            tracing::info!("init client connect to {}", ip);
            // let mut client = PeerCommunicationClient::connect(ip).await?;
            let (send_to_server, server_receiver) = channel::<YuxiMsg>(100);
            //init client
            let mut client = RpcClient::new(ip.clone(), sender.clone()).await;
            tokio::spawn(async move {
                client.run_client(server_receiver).await;
            });
            self.servers.insert(id.clone(), send_to_server);
        }
    }
}
