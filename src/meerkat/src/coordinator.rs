use std::{collections::HashMap, time::Duration};

use common::{config::Config, get_local_time, ycsb::YcsbQuery, CID_LEN};
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    meerkat::{meerkat_client::MeerkatClient, MeerkatMsg},
};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{channel, unbounded_channel, Receiver, Sender},
    time::{sleep, Instant},
};
use tonic::transport::Channel;

use crate::peer_communication::RpcClient;

static RETRY: i32 = 20;

pub struct MeerkatCoordinator {
    config: Config,
    id: u32,
    txn_id: u64,
    // sharded txn
    txn: MeerkatMsg,
    // send to servers
    servers: HashMap<u32, Sender<MeerkatMsg>>,
    workload: YcsbQuery,
    txns_per_client: i32,
    recv: Receiver<MeerkatMsg>,
}

impl MeerkatCoordinator {
    pub fn new(
        id: u32,
        config: Config,
        read_perc: i32,
        txns_per_client: i32,
        recv: Receiver<MeerkatMsg>,
    ) -> Self {
        Self {
            id,
            txn_id: (id as u64) << CID_LEN,
            txn: MeerkatMsg::default(),
            servers: HashMap::new(),
            workload: YcsbQuery::new(config.zipf_theta, config.req_per_query as i32, read_perc),
            config,
            txns_per_client,
            recv,
        }
    }

    pub async fn init_run(&mut self, sender: Sender<MeerkatMsg>) {
        // self.init_workload();
        self.init_rpc(sender).await;
        println!("init rpc done");
        // run transactions
        let mut latency_result = Vec::new();
        // send msgs
        let total_start = Instant::now();
        for i in 0..1 {
            self.workload.generate();
            let start = Instant::now();
            let mut j = 0;
            while j < RETRY {
                if self.run_transaction().await {
                    break;
                }
                j += 1;
            }
            let end_time = start.elapsed().as_micros();
            println!("latency time = {}", end_time);
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

    async fn broadcast(&mut self, msg: MeerkatMsg) {
        for (id, server) in self.servers.iter() {
            server.send(msg.clone()).await;
        }
    }

    async fn send_to_server(&mut self, msg: MeerkatMsg, id: u32) {
        self.servers.get(&id).unwrap().send(msg).await;
    }

    async fn run_transaction(&mut self) -> bool {
        let timestamp = get_local_time(self.id);
        // get the read set from server
        // execute phase
        let read_server_index = self.id % 3;
        let mut client = self.servers.get(&read_server_index).unwrap().clone();
        let read_request = MeerkatMsg {
            txn_id: self.txn_id,
            read_set: self.txn.read_set.clone(),
            write_set: Vec::new(),
            executor_id: 0,
            op: TxnOp::ReadOnly.into(),
            from: self.id,
            timestamp,
            txn_type: Some(TxnType::Ycsb.into()),
        };
        // let result = client.txn_msg(read_request).await.unwrap().into_inner();
        self.send_to_server(read_request, read_server_index).await;
        // wait for result
        let result = self.recv.recv().await.unwrap();
        self.txn.read_set = result.read_set;
        self.txn.op = TxnOp::Prepare.into();
        // validate phase
        // prepare, prepare will send to all the server in the shard
        self.broadcast(self.txn.clone()).await;
        // for (id, server) in self.servers.iter() {
        //     let mut server_client = server.clone();
        //     let validate = self.txn.clone();
        //     let sender = result_sender.clone();
        //     tokio::spawn(async move {
        //         let result = server_client.txn_msg(validate).await.unwrap().into_inner();
        //         sender.send(result);
        //     });
        // }

        // handle prepare response
        for i in 0..3 {
            let prepare_res = self.recv.recv().await.unwrap();
            if prepare_res.op() == TxnOp::Abort.into() {
                // abort all the txn
                return false;
            }
        }
        // txn success
        // broadcast commit
        self.txn.op = TxnOp::Commit.into();
        for read in self.txn.read_set.iter_mut() {
            read.value = None;
        }
        self.broadcast(self.txn.clone()).await;
        return true;
    }
    // pub async fn init_rpc(&mut self) {
    //     // hold the clients to all the server
    //     for (id, server_addr) in self.config.server_addrs.iter() {
    //         println!("connect to {}-{}", id, server_addr);
    //         loop {
    //             match MeerkatClient::connect(server_addr.clone()).await {
    //                 Ok(client) => {
    //                     self.servers.insert(*id, client);
    //                     break;
    //                 }
    //                 Err(e) => {
    //                     println!("{}", e);
    //                     sleep(Duration::from_millis(100)).await;
    //                 }
    //             }
    //         }
    //     }
    // }

    pub async fn init_rpc(&mut self, sender: Sender<MeerkatMsg>) {
        // init rpc client to connect to other peers
        for (id, ip) in self.config.server_addrs.iter() {
            tracing::info!("init client connect to {}", ip);
            // let mut client = PeerCommunicationClient::connect(ip).await?;
            let (send_to_server, server_receiver) = channel::<MeerkatMsg>(100);
            //init client
            let mut client = RpcClient::new(ip.clone(), sender.clone()).await;
            tokio::spawn(async move {
                client.run_client(server_receiver).await;
            });
            self.servers.insert(id.clone(), send_to_server);
        }
    }

    ///////////////////////
    // tpcc txns
    // async fn get_subscriber_data(&mut self,  query: GetSubscriberDataQuery) -> bool {
    //     let s_id = query.s_id;
    //     let timestamp = get_local_time(self.id);
    //     let mut result_num: i32 = 0;
    //     let (sender, mut receiver) = unbounded_channel::<MeerkatMsg>();
    //     // get the read set from server
    //     let read_server_index = self.id % 3;
    //     let shard = self.shard_the_transaction(read_set, write_set)
    //     for (shard, per_server) in self.txn.iter() {
    //         if per_server.read_set.len() > 0 {
    //             result_num += 1;
    //             let server_id = self
    //                 .config
    //                 .shards
    //                 .get(shard)
    //                 .unwrap()
    //                 .get(read_server_index as usize)
    //                 .unwrap();
    //             let mut client = self.servers.get(server_id).unwrap().clone();
    //             let result_sender = sender.clone();
    //             let read_request = MeerkatMsg {
    //                 txn_id: self.txn_id,
    //                 read_set: per_server.read_set.clone(),
    //                 write_set: Vec::new(),
    //                 executor_id: 0,
    //                 op: TxnOp::TRead.into(),
    //                 from: self.id,
    //                 timestamp,
    //             };
    //             tokio::spawn(async move {
    //                 let result = client.txn_msg(read_request).await.unwrap().into_inner();
    //                 result_sender.send(result);
    //             });
    //         }
    //     }
    //     // while result_num
    //     while result_num > 0 {
    //         result_num -= 1;
    //         let read = receiver.recv().await.unwrap();
    //         match self.txn.get_mut(&read.from) {
    //             Some(msg) => {
    //                 msg.read_set = read.read_set;
    //             }
    //             None => todo!(),
    //         }
    //     }
    //     // prepare, prepare will send to all the server in the shard
    //     result_num = (self.txn.len() * 3) as i32;
    //     for (shard, per_server) in self.txn.iter() {
    //         let server_ids = self.config.shards.get(shard).unwrap();
    //         for server_id in server_ids.iter() {
    //             let mut client = self.servers.get(server_id).unwrap().clone();
    //             let result_sender = sender.clone();
    //             let read_request = MeerkatMsg {
    //                 txn_id: self.txn_id,
    //                 read_set: per_server.read_set.clone(),
    //                 write_set: per_server.write_set.clone(),
    //                 executor_id: 0,
    //                 op: TxnOp::TPrepare.into(),
    //                 from: self.id,
    //                 timestamp,
    //             };
    //             tokio::spawn(async move {
    //                 let result = client.txn_msg(read_request).await.unwrap().into_inner();
    //                 result_sender.send(result);
    //             });
    //         }
    //     }
    //     // handle prepare response
    //     while result_num > 0 {
    //         result_num -= 1;
    //         let prepare_res = receiver.recv().await.unwrap();
    //         if prepare_res.op == TxnOp::TAbort.into() {
    //             // abort all the txn
    //             return false;
    //         }
    //     }
    //     // txn success
    //     true
    // }
}
