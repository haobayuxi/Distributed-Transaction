use std::{collections::HashMap, sync::Arc};

use common::get_txnid;
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::sync::{mpsc::UnboundedReceiver, RwLock};

use crate::{dep_graph::TXNS, JanusMeta, Msg};

pub struct Apply {
    recv: UnboundedReceiver<u64>,
    mem: HashMap<i64, String>,
    server_id: u32,
}

impl Apply {
    pub fn new(recv: UnboundedReceiver<u64>, mem: HashMap<i64, String>, server_id: u32) -> Self {
        Self {
            recv,
            mem,
            server_id,
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    self.handle_execute(msg).await;
                }
                None => continue,
            }
        }
    }

    async fn handle_execute(&mut self, txnid: u64) {
        println!("execute txn {:?}", get_txnid(txnid));
        let (clientid, index) = get_txnid(txnid);
        unsafe {
            let node = &mut TXNS[clientid as usize][index as usize];
            // execute
            let mut result = JanusMsg {
                txn_id: txnid,
                read_set: Vec::new(),
                write_set: Vec::new(),
                op: TxnOp::CommitRes.into(),
                from: self.server_id,
                deps: Vec::new(),
                txn_type: None,
            };

            for read in node.txn.read_set.iter() {
                let read_result = ReadStruct {
                    key: read.key.clone(),
                    value: Some(self.mem.get(&read.key).unwrap().clone()),
                    timestamp: None,
                };
                result.read_set.push(read_result);
            }

            for write in node.txn.write_set.iter() {
                self.mem.insert(write.key, write.value.clone());
            }

            // reply to coordinator
            if node.txn.from % 3 == self.server_id {
                node.callback.take().unwrap().send(Ok(result)).await;
            }
        }
    }
}
