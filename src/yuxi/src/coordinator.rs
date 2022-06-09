use std::{collections::HashMap, time::Duration};

use common::{config::Config, get_local_time, ycsb::YcsbQuery, SHARD_NUM};
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    yuxi::{yuxi_client::YuxiClient, YuxiMsg},
};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tonic::transport::Channel;

static RETRY: i32 = 20;

pub struct YuxiCoordinator {
    config: Config,
    read_only: bool,
    id: i32,
    txn_id: i64,
    // sharded txn
    txn: HashMap<i32, YuxiMsg>,
    // send to servers
    servers: HashMap<i32, YuxiClient<Channel>>,
    workload: YcsbQuery,
}

impl YuxiCoordinator {
    pub fn new(id: i32, config: Config, read_perc: i32) -> Self {
        Self {
            read_only: false,
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
            let read_struct = ReadStruct {
                key: read,
                value: None,
                timestamp: None,
            };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.read_set.push(read_struct);
            } else {
                let msg = YuxiMsg {
                    txn_id: self.txn_id,
                    read_set: vec![read_struct],
                    write_set: Vec::new(),
                    op: TxnOp::Prepare.into(),
                    from: self.id,
                    timestamp: 0,
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
                // timestamp: Some(0),
            };
            if self.txn.contains_key(&shard) {
                let msg = self.txn.get_mut(&shard).unwrap();

                msg.write_set.push(write_struct);
            } else {
                let msg = YuxiMsg {
                    txn_id: self.txn_id,
                    read_set: Vec::new(),
                    write_set: vec![write_struct],
                    op: TxnOp::Prepare.into(),
                    from: self.id,
                    timestamp: 0,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                self.txn.insert(shard, msg);
            }
        }
    }

    async fn run_transaction(&mut self) -> bool {
        // init ts
        let timestamp = get_local_time(0);
        let mut result_num: i32 = 0;
        let (sender, mut receiver) = unbounded_channel::<YuxiMsg>();

        // prepare, prepare will send to all the server in the shard
        result_num = (self.txn.len() * 3) as i32;
        for (shard, per_server) in self.txn.iter() {
            let server_ids = self.config.shards.get(shard).unwrap();
            for server_id in server_ids.iter() {
                let mut client = self.servers.get(server_id).unwrap().clone();
                let result_sender = sender.clone();
                let read_request = YuxiMsg {
                    txn_id: self.txn_id,
                    read_set: per_server.read_set.clone(),
                    write_set: per_server.write_set.clone(),
                    op: TxnOp::Prepare.into(),
                    from: self.id,
                    timestamp,
                    txn_type: Some(TxnType::Ycsb.into()),
                };
                tokio::spawn(async move {
                    let result = client.yuxi_txn(read_request).await.unwrap().into_inner();
                    result_sender.send(result);
                });
            }
        }
        // handle prepare response
        while result_num > 0 {
            result_num -= 1;
            let prepare_res = receiver.recv().await.unwrap();
            if prepare_res.op() == TxnOp::Abort {
                // abort all the txn
                return false;
            }
        }
        let commit = false;
        if !commit {
            // accept
        }

        // broadcast commit

        // txn success
        return true;
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