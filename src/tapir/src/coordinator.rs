use std::{collections::HashMap, time::Duration};

use common::config::Config;
use rpc::tapir::{tapir_client::TapirClient, ReadStruct, TapirMsg, TxnOp, WriteStruct};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tonic::transport::Channel;

use crate::peer_communication::RpcClient;

pub struct TapirCoordinator {
    // replica_id: i32,
    read_optimize: bool,
    id: i32,
    txn_id: i64,
    txn: HashMap<i32, TapirMsg>,
    // send to servers
    servers: HashMap<i32, TapirClient<Channel>>,
}

impl TapirCoordinator {
    pub fn new() {}

    async fn run_transaction(&mut self) -> bool {
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
    pub async fn init_rpc(&mut self, config: Config) {
        // hold the clients to all the server
        for (id, server_addr) in config.server_addrs {
            loop {
                match TapirClient::connect(server_addr.clone()).await {
                    Ok(client) => {
                        self.servers.insert(id, client);
                    }
                    Err(_) => {
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    fn shard_the_transaction(&mut self, read_set: Vec<String>, write_set: Vec<(String, String)>) {}
}
