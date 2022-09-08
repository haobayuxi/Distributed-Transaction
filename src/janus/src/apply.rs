use std::{collections::HashMap, sync::Arc, time::Duration};

use common::get_txnid;
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::{
    sync::{mpsc::UnboundedReceiver, RwLock},
    time::sleep,
};

use crate::{dep_graph::TXNS, peer::META, JanusMeta, Msg};

pub struct Apply {
    pub mem_index: Arc<HashMap<i64, usize>>,
    pub msg: Msg,
    pub is_reply: bool,
}

impl Apply {
    pub async fn run(&self) {
        unsafe {
            for dep in self.msg.txn.deps.iter() {
                if *dep == 0 {
                    continue;
                }
                let (dep_clientid, dep_index) = get_txnid(*dep);
                while TXNS[dep_clientid as usize].len() <= dep_index as usize
                    || !TXNS[dep_clientid as usize][dep_index as usize].executed
                {
                    // not executed
                    sleep(Duration::from_nanos(1000));
                }
            }
            // execute
            let mut result = JanusMsg {
                txn_id: self.msg.txn.txn_id,
                read_set: Vec::new(),
                write_set: Vec::new(),
                op: TxnOp::CommitRes.into(),
                from: 0,
                deps: Vec::new(),
                txn_type: None,
            };

            for read in self.msg.txn.read_set.iter() {
                let index = self.mem_index.get(&read.key).unwrap();
                let read_result = ReadStruct {
                    key: read.key.clone(),
                    value: Some(META[*index].1.read().await.clone()),
                    timestamp: None,
                };
                result.read_set.push(read_result);
            }

            for write in self.msg.txn.write_set.iter() {
                let index = self.mem_index.get(&write.key).unwrap();
                let mut data = META[*index].1.write().await;
                *data = write.value.clone();
            }

            // reply to coordinator
            if self.is_reply {
                println!("execute {:?}", get_txnid(result.txn_id));
                self.msg.callback.send(Ok(result)).await;
            }
        }
    }
}

// pub struct Apply {
//     recv: UnboundedReceiver<u64>,
//     mem: HashMap<i64, String>,
//     server_id: u32,
// }

// impl Apply {
//     pub fn new(recv: UnboundedReceiver<u64>, mem: HashMap<i64, String>, server_id: u32) -> Self {
//         Self {
//             recv,
//             mem,
//             server_id,
//         }
//     }

//     pub async fn run(&mut self) {
//         loop {
//             match self.recv.recv().await {
//                 Some(msg) => {
//                     self.handle_execute(msg).await;
//                 }
//                 None => continue,
//             }
//         }
//     }

//     async fn handle_execute(&mut self, txnid: u64) {
//         // println!("execute txn {:?}", get_txnid(txnid));
//         let (clientid, index) = get_txnid(txnid);
//         unsafe {
//             let node = &mut TXNS[clientid as usize][index as usize];
//             // execute
//             let mut result = JanusMsg {
//                 txn_id: txnid,
//                 read_set: Vec::new(),
//                 write_set: Vec::new(),
//                 op: TxnOp::CommitRes.into(),
//                 from: self.server_id,
//                 deps: Vec::new(),
//                 txn_type: None,
//             };

//             for read in node.txn.read_set.iter() {
//                 let read_result = ReadStruct {
//                     key: read.key.clone(),
//                     value: Some(self.mem.get(&read.key).unwrap().clone()),
//                     timestamp: None,
//                 };
//                 result.read_set.push(read_result);
//             }

//             for write in node.txn.write_set.iter() {
//                 self.mem.insert(write.key, write.value.clone());
//             }

//             // reply to coordinator
//             if node.txn.from % 3 == self.server_id {
//                 node.callback.take().unwrap().send(Ok(result)).await;
//             }
//         }
//     }
// }
