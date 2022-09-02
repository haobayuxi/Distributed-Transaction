use std::{collections::HashMap, time::Duration};

use common::{config::Config, get_local_time, ycsb::YcsbQuery, SHARD_NUM};
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    yuxi::{yuxi_client::YuxiClient, YuxiMsg},
};
use tokio::{
    sync::mpsc::{unbounded_channel, Receiver, Sender},
    time::sleep,
};
use tonic::transport::Channel;

static RETRY: i32 = 20;

pub struct YuxiCoordinator {
    config: Config,
    read_only: bool,
    is_ycsb: bool,
    id: i32,
    txn_id: i64,
    // sharded txn
    txn: YuxiMsg,
    // send to servers
    servers: HashMap<i32, Sender<YuxiMsg>>,
    recv: Receiver<YuxiMsg>,
    workload: YcsbQuery,
}

impl YuxiCoordinator {
    pub fn new(id: i32, config: Config, read_perc: i32, recv: Receiver<YuxiMsg>) -> Self {
        Self {
            read_only: false,
            is_ycsb: true,
            id,
            txn_id: 0,
            txn: YuxiMsg::default(),
            servers: HashMap::new(),
            recv,
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

    // fn get_servers_by_shardid(&mut self, shard: i32) -> Vec<i32> {
    //     self.config.shards.get(&shard).unwrap().clone()
    // }

    // fn shard_the_transaction(&mut self, read_set: Vec<i64>, write_set: Vec<(i64, String)>) {
    //     self.txn.clear();
    //     // group read write into multi shards, try to read from one of the server
    //     for read in read_set {
    //         let shard = (read as i32) % SHARD_NUM;
    //         let read_struct = ReadStruct {
    //             key: read,
    //             value: None,
    //             timestamp: None,
    //         };
    //         if self.txn.contains_key(&shard) {
    //             let msg = self.txn.get_mut(&shard).unwrap();

    //             msg.read_set.push(read_struct);
    //         } else {
    //             let msg = YuxiMsg {
    //                 txn_id: self.txn_id,
    //                 read_set: vec![read_struct],
    //                 write_set: Vec::new(),
    //                 op: TxnOp::Prepare.into(),
    //                 from: self.id,
    //                 timestamp: 0,
    //                 txn_type: None,
    //             };
    //             self.txn.insert(shard, msg);
    //         }
    //     }

    //     for (key, value) in write_set {
    //         let shard = (key as i32) % SHARD_NUM;
    //         let write_struct = WriteStruct {
    //             key,
    //             value,
    //             // timestamp: Some(0),
    //         };
    //         if self.txn.contains_key(&shard) {
    //             let msg = self.txn.get_mut(&shard).unwrap();

    //             msg.write_set.push(write_struct);
    //         } else {
    //             let msg = YuxiMsg {
    //                 txn_id: self.txn_id,
    //                 read_set: Vec::new(),
    //                 write_set: vec![write_struct],
    //                 op: TxnOp::Prepare.into(),
    //                 from: self.id,
    //                 timestamp: 0,
    //                 txn_type: Some(TxnType::Ycsb.into()),
    //             };
    //             self.txn.insert(shard, msg);
    //         }
    //     }
    // }

    async fn run_transaction(&mut self) -> bool {
        // init ts
        let timestamp = get_local_time(0);

        // prepare, prepare will send to all the server
        self.txn = YuxiMsg {
            txn_id: self.txn_id,
            read_set: read_set.clone(),
            write_set: write_set.clone(),
            op: TxnOp::Prepare.into(),
            from: self.id,
            timestamp,
            txn_type: self.txn.txn_type,
        };
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
                op: TxnOp::Commit.into(),
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

    pub async fn init_rpc(&mut self) {
        // hold the clients to all the server
        for (id, server_addr) in self.config.server_addrs.iter() {
            println!("connect to {}-{}", id, server_addr);
            loop {
                match YuxiClient::connect(server_addr.clone()).await {
                    Ok(client) => {
                        self.servers.insert(*id, client);
                        break;
                    }
                    Err(e) => {
                        println!("{}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}
