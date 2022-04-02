use std::{collections::HashMap, time::Duration};

use common::{
    config::{self, Config},
    get_local_time, SHARD_NUM,
};
use rpc::tapir::{tapir_client::TapirClient, ReadStruct, TapirMsg, TxnOp, WriteStruct};
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
    workload: Vec<(Vec<ReadStruct>, Vec<WriteStruct>)>,
}

impl TapirCoordinator {
    pub fn new(id: i32, read_optimize: bool, config: Config) -> Self {
        Self {
            read_optimize,
            id,
            txn_id: 0,
            txn: HashMap::new(),
            servers: HashMap::new(),
            workload: Vec::new(),
            config,
        }
    }

    pub async fn init_run(&mut self) {
        self.init_workload();
        self.init_rpc().await;
        // run transactions
        for txn in self.workload.iter() {}
    }

    fn get_servers_by_shardid(&mut self, shard: i32) -> Vec<i32> {
        self.config.shards.get(&shard).unwrap().clone()
    }

    fn shard_the_transaction(&mut self, read_set: Vec<i64>, write_set: Vec<(i64, String)>) {
        self.txn.clear();
        // group read write into multi shards, try to read from one of the server
        for read in read_set {
            let shard = (read as i32) % SHARD_NUM;
            let serverids = self.get_servers_by_shardid(shard);
            
            for server_id = 
            if result.contains_key(&executor_id) {
                let msg = result.get_mut(&executor_id).unwrap();
                msg.read_set.push(read);
            } else {
                let msg = JanusMsg {
                    txn_id: txn.txn_id,
                    read_set: vec![read],
                    write_set: Vec::new(),
                    executor_ids: Vec::new(),
                    op: txn.op,
                    from: txn.from,
                    deps: txn.deps.clone(),
                };
                result.insert(executor_id, msg);
            }
        }

        for write in txn.write_set {
            let executor_id = (write.key as i32) % EXECUTOR_NUM;
            if result.contains_key(&executor_id) {
                let msg = result.get_mut(&executor_id).unwrap();
                msg.write_set.push(write);
            } else {
                let msg = JanusMsg {
                    txn_id: txn.txn_id,
                    read_set: Vec::new(),
                    write_set: vec![write],
                    executor_ids: Vec::new(),
                    op: txn.op,
                    from: txn.from,
                    deps: txn.deps.clone(),
                };
                result.insert(executor_id, msg);
            }
        }
    }

    fn init_workload(&mut self) {
        let readstruct = ReadStruct {
            key: 100,
            value: None,
            timestamp: None,
        };
        let read_vec = vec![readstruct];
        self.workload = vec![(read_vec, Vec::new()); 100];
    }

    async fn run_transaction(&mut self) -> bool {
        let timestamp = get_local_time(self.id);
        let mut result_num: i32 = 0;
        let (sender, mut receiver) = unbounded_channel::<TapirMsg>();
        // get the read set
        for (server_id, per_server) in self.txn.iter() {
            if per_server.read_set.len() > 0 {
                result_num += 1;
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
        // prepare
        result_num = self.txn.len() as i32;
        for (server_id, per_server) in self.txn.iter() {
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
            };
            tokio::spawn(async move {
                let result = client.txn_msg(read_request).await.unwrap().into_inner();
                result_sender.send(result);
            });
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
            loop {
                match TapirClient::connect(server_addr.clone()).await {
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
}
