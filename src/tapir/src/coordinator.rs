use std::{collections::HashMap, time::Duration};

use common::{config::Config, get_local_time, ycsb::YcsbQuery, SHARD_NUM};
use rpc::{
    common::{ReadStruct, WriteStruct},
    tapir::{tapir_client::TapirClient, TapirMsg, TapirReadStruct, TxnOp, TxnType},
};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tonic::transport::Channel;

static RETRY: i32 = 20;

pub struct TapirCoordinator {
    config: Config,
    read_optimize: bool,
    id: i32,
    txn_id: i64,
    // sharded txn
    txn: HashMap<i32, TapirMsg>,
    // send to servers
    servers: HashMap<i32, TapirClient<Channel>>,
    workload: YcsbQuery,
}

impl TapirCoordinator {
    pub fn new(id: i32, read_optimize: bool, config: Config, read_perc: i32) -> Self {
        Self {
            read_optimize,
            id,
            txn_id: 0,
            txn: HashMap::new(),
            servers: HashMap::new(),
            workload: YcsbQuery::new(
                config.zipf_theta,
                config.table_size,
                config.req_per_query as i32,
                read_perc,
            ),
            config,
        }
    }

    pub async fn init_run(&mut self) {
        // self.init_workload();
        self.init_rpc().await;
        println!("init rpc done");
        // run transactions
        for i in 0..100 {
            self.workload.generate();
            if self.run_transaction().await {
                println!("success {}", i);
            } else {
                println!("fail {}", i);
            }
        }
    }

    fn get_servers_by_shardid(&mut self, shard: i32) -> Vec<i32> {
        self.config.shards.get(&shard).unwrap().clone()
    }

    fn shard_the_transaction(&mut self, read_set: Vec<i64>, write_set: Vec<(i64, String)>) {
        self.txn.clear();
        // group read write into multi shards, try to read from one of the server
        for read in read_set {
            let shard = (read as i32) % SHARD_NUM;
            let read_struct = TapirReadStruct {
                read: Some(ReadStruct {
                    key: read,
                    value: None,
                }),
                timestamp: None,
            };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.read_set.push(read_struct);
            } else {
                let msg = TapirMsg {
                    txn_id: self.txn_id,
                    read_set: vec![read_struct],
                    write_set: Vec::new(),
                    executor_id: 0,
                    op: TxnOp::TPrepare.into(),
                    from: self.id,
                    timestamp: 0,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                self.txn.insert(shard, msg);
            }
        }

        for (key, value) in write_set {
            let shard = (key as i32) % SHARD_NUM;
            let write_struct = WriteStruct { key, value };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.write_set.push(write_struct);
            } else {
                let msg = TapirMsg {
                    txn_id: self.txn_id,
                    read_set: Vec::new(),
                    write_set: vec![write_struct],
                    executor_id: 0,
                    op: TxnOp::TPrepare.into(),
                    from: self.id,
                    timestamp: 0,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                self.txn.insert(shard, msg);
            }
        }
    }

    async fn run_transaction(&mut self) -> bool {
        let timestamp = get_local_time(self.id);
        let mut result_num: i32 = 0;
        let (sender, mut receiver) = unbounded_channel::<TapirMsg>();
        // get the read set from server
        let read_server_index = self.id % 3;
        for (shard, per_server) in self.txn.iter() {
            if per_server.read_set.len() > 0 {
                result_num += 1;
                let server_id = self
                    .config
                    .shards
                    .get(shard)
                    .unwrap()
                    .get(read_server_index as usize)
                    .unwrap();
                let mut client = self.servers.get(server_id).unwrap().clone();
                let result_sender = sender.clone();
                let read_request = TapirMsg {
                    txn_id: self.txn_id,
                    read_set: per_server.read_set.clone(),
                    write_set: Vec::new(),
                    executor_id: 0,
                    op: TxnOp::TRead.into(),
                    from: self.id,
                    timestamp,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                tokio::spawn(async move {
                    let result = client.txn_msg(read_request).await.unwrap().into_inner();
                    result_sender.send(result);
                });
            }
        }
        // while result_num
        while result_num > 0 {
            result_num -= 1;
            let read = receiver.recv().await.unwrap();
            match self.txn.get_mut(&read.from) {
                Some(msg) => {
                    msg.read_set = read.read_set;
                }
                None => todo!(),
            }
        }
        // prepare, prepare will send to all the server in the shard
        result_num = (self.txn.len() * 3) as i32;
        for (shard, per_server) in self.txn.iter() {
            let server_ids = self.config.shards.get(shard).unwrap();
            for server_id in server_ids.iter() {
                let mut client = self.servers.get(server_id).unwrap().clone();
                let result_sender = sender.clone();
                let read_request = TapirMsg {
                    txn_id: self.txn_id,
                    read_set: per_server.read_set.clone(),
                    write_set: per_server.write_set.clone(),
                    executor_id: 0,
                    op: TxnOp::TPrepare.into(),
                    from: self.id,
                    timestamp,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                tokio::spawn(async move {
                    let result = client.txn_msg(read_request).await.unwrap().into_inner();
                    result_sender.send(result);
                });
            }
        }
        // handle prepare response
        while result_num > 0 {
            result_num -= 1;
            let prepare_res = receiver.recv().await.unwrap();
            if prepare_res.op == TxnOp::TAbort.into() {
                // abort all the txn
                return false;
            }
        }
        // txn success
        return true;
    }
    pub async fn init_rpc(&mut self) {
        // hold the clients to all the server
        for (id, server_addr) in self.config.server_addrs.iter() {
            println!("connect to {}-{}", id, server_addr);
            loop {
                match TapirClient::connect(server_addr.clone()).await {
                    Ok(client) => {
                        self.servers.insert(*id, client);
                    }
                    Err(e) => {
                        println!("{}", e);
                        sleep(Duration::from_millis(1000)).await;
                    }
                }
            }
        }
    }

    ///////////////////////
    // tpcc txns
    // async fn get_subscriber_data(&mut self,  query: GetSubscriberDataQuery) -> bool {
    //     let s_id = query.s_id;
    //     let timestamp = get_local_time(self.id);
    //     let mut result_num: i32 = 0;
    //     let (sender, mut receiver) = unbounded_channel::<TapirMsg>();
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
    //             let read_request = TapirMsg {
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
    //             let read_request = TapirMsg {
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
