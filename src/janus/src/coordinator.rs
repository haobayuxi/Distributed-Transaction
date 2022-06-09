use std::{collections::HashMap, time::Duration};

use common::{config::Config, ycsb::YcsbQuery, SHARD_NUM};
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    janus::{janus_client::JanusClient, JanusMsg},
};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tonic::transport::Channel;

pub struct JanusCoordinator {
    // replica_id: i32,
    read_optimize: bool,
    id: i32,
    txn_id: i64,
    txn: HashMap<i32, JanusMsg>,
    workload: YcsbQuery,
    // send to servers
    servers: HashMap<i32, JanusClient<Channel>>,
    config: Config,
}

impl JanusCoordinator {
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

    async fn run_transaction(&mut self) -> bool {
        let mut result_num: i32 = 0;
        let (sender, mut receiver) = unbounded_channel::<JanusMsg>();

        // prepare
        result_num = self.txn.len() as i32;
        for (server_id, per_server) in self.txn.iter() {
            let mut client = self.servers.get(server_id).unwrap().clone();
            let result_sender = sender.clone();
            let read_request = JanusMsg {
                txn_id: self.txn_id,
                read_set: per_server.read_set.clone(),
                write_set: per_server.write_set.clone(),
                executor_ids: Vec::new(),
                op: TxnOp::Prepare.into(),
                from: self.id,
                deps: Vec::new(),
                txn_type: Some(TxnType::Ycsb.into()),
            };
            tokio::spawn(async move {
                let result = client.janus_txn(read_request).await.unwrap().into_inner();
                result_sender.send(result);
            });
        }
        // handle prepare response
        while result_num > 0 {
            result_num -= 1;
            let prepare_res = receiver.recv().await.unwrap();
            if prepare_res.op() == TxnOp::Accept {
                // abort all the txn
                return false;
            }
        }
        // txn success
        return true;
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

    async fn init_rpc(&mut self) {
        // hold the clients to all the server
        for (id, server_addr) in self.config.server_addrs.iter() {
            loop {
                match JanusClient::connect(server_addr.clone()).await {
                    Ok(client) => {
                        self.servers.insert(*id, client);
                    }
                    Err(_) => {
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    fn shard_the_transaction(&mut self, read_set: Vec<i64>, write_set: Vec<(i64, String)>) {
        self.txn.clear();
        // group read write into multi shards, try to read from one of the server
        for read in read_set {
            let shard = (read as i32) % SHARD_NUM;
            let read_struct = ReadStruct {
                key: read,
                value: None,
                timestamp: None,
            };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.read_set.push(read_struct);
            } else {
                let msg = JanusMsg {
                    txn_id: self.txn_id,
                    read_set: vec![read_struct],
                    write_set: Vec::new(),
                    op: TxnOp::Prepare.into(),
                    from: self.id,
                    executor_ids: Vec::new(),
                    deps: Vec::new(),
                    txn_type: None,
                };
                self.txn.insert(shard, msg);
            }
        }

        for (key, value) in write_set {
            let shard = (key as i32) % SHARD_NUM;
            let write_struct = WriteStruct {
                key,
                value,
                // timestamp: None,
            };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.write_set.push(write_struct);
            } else {
                let msg = JanusMsg {
                    txn_id: self.txn_id,
                    read_set: Vec::new(),
                    write_set: vec![write_struct],
                    executor_ids: Vec::new(),
                    op: TxnOp::Prepare.into(),
                    from: self.id,
                    deps: Vec::new(),
                    txn_type: None,
                };
                self.txn.insert(shard, msg);
            }
        }
    }
}
