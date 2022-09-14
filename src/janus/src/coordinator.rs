use std::{collections::HashMap, time::Duration};

use common::{config::Config, ycsb::YcsbQuery, CID_LEN};
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    janus::{janus_client::JanusClient, JanusMsg},
};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{channel, unbounded_channel, Receiver, Sender},
    time::{sleep, Instant},
};
use tonic::transport::Channel;

use crate::peer_communication::RpcClient;

pub struct JanusCoordinator {
    // replica_id: i32,
    // read_optimize: bool,
    id: u32,
    txn_id: u64,
    txn: JanusMsg,
    workload: YcsbQuery,
    // send to servers
    servers: HashMap<u32, Sender<JanusMsg>>,
    config: Config,
    txns_per_client: i32,
    recv: Receiver<JanusMsg>,
}

impl JanusCoordinator {
    pub fn new(
        id: u32,
        config: Config,
        txns_per_client: i32,
        read_perc: i32,
        recv: Receiver<JanusMsg>,
        zipf_theta: f64,
    ) -> Self {
        Self {
            // read_optimize,
            id,
            txn_id: (id as u64) << CID_LEN,
            txn: JanusMsg::default(),
            servers: HashMap::new(),
            workload: YcsbQuery::new(zipf_theta, config.req_per_query as i32, read_perc),
            config,
            txns_per_client,
            recv,
        }
    }

    async fn run_transaction(&mut self) -> bool {
        // prepare

        // println!("prepare");
        self.txn.deps.clear();
        self.txn.read_set = self.workload.read_set.clone();
        self.txn.write_set = self.workload.write_set.clone();
        self.txn.from = self.id;
        self.txn.op = TxnOp::Prepare.into();
        self.txn.txn_id = self.txn_id;
        self.broadcast(self.txn.clone()).await;
        // handle prepare response
        let mut result = self.recv.recv().await.unwrap();
        let mut fast_commit = true;
        for i in 0..2 {
            let prepare_res = self.recv.recv().await.unwrap();
            if result.deps != prepare_res.deps {
                fast_commit = false;
                let result_vec = result.deps.clone();
                for iter in prepare_res.deps.into_iter() {
                    if !result_vec.contains(&iter) {
                        result.deps.push(iter);
                    }
                }
            }
        }
        self.txn.deps = result.deps;
        // println!("deps {:?}", self.txn.deps);
        if !fast_commit {
            // accept
            // println!("accept");
            let mut accept = self.txn.clone();
            accept.read_set.clear();
            accept.write_set.clear();
            accept.op = TxnOp::Accept.into();
            self.broadcast(accept).await;
            for i in 0..3 {
                self.recv.recv().await;
            }
        }
        // txn success
        let mut commit = self.txn.clone();
        commit.op = TxnOp::Commit.into();
        self.broadcast(commit).await;
        self.recv.recv().await;
        return true;
    }

    async fn broadcast(&mut self, msg: JanusMsg) {
        for (id, server) in self.servers.iter() {
            server.send(msg.clone()).await;
        }
    }

    pub async fn init_run(&mut self, sender: Sender<JanusMsg>) {
        // self.init_workload();
        self.init_rpc(sender).await;
        println!("init rpc done");
        // run transactions
        self.txn.from = self.id;
        self.txn.txn_type = Some(TxnType::Ycsb.into());
        let mut latency_result = Vec::new();
        // send msgs
        let total_start = Instant::now();
        for i in 0..self.txns_per_client {
            self.workload.generate();
            let start = Instant::now();
            self.run_transaction().await;
            self.txn_id += 1;
            let end_time = start.elapsed().as_micros();
            // println!("latency time = {}", end_time);
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

    pub async fn init_rpc(&mut self, sender: Sender<JanusMsg>) {
        // init rpc client to connect to other peers
        for (id, ip) in self.config.server_addrs.iter() {
            println!("init client connect to {}", ip);
            // let mut client = PeerCommunicationClient::connect(ip).await?;
            let (send_to_server, server_receiver) = channel::<JanusMsg>(100);
            //init client
            let mut client = RpcClient::new(ip.clone(), sender.clone()).await;
            tokio::spawn(async move {
                client.run_client(server_receiver).await;
            });
            self.servers.insert(id.clone(), send_to_server);
        }
    }
}
