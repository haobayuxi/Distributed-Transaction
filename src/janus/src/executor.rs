use std::{collections::HashMap, sync::Arc};

use common::get_txnid;
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::sync::{
    mpsc::{Sender, UnboundedReceiver},
    RwLock,
};

use crate::{JanusMeta, Msg};

pub struct Executor {
    server_id: u32,
    // memory
    mem: HashMap<i64, RwLock<(JanusMeta, String)>>,
    // txns
    txns: HashMap<u64, JanusMsg>,
    //
    // recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new(server_id: u32, mem: HashMap<i64, RwLock<(JanusMeta, String)>>) -> Self {
        Self {
            server_id,
            mem,
            txns: HashMap::new(),
            // recv,
        }
    }

    pub async fn handle_msg(&mut self, msg: Msg) {
        match msg.txn.op() {
            TxnOp::ReadOnly => self.handle_prepare(msg).await,
            TxnOp::Prepare => self.handle_prepare(msg).await,
            TxnOp::Accept => self.handle_accept(msg).await,
            TxnOp::Commit => self.handle_execute(msg).await,
            TxnOp::ReadOnlyRes => {}
            TxnOp::PrepareRes => {}
            TxnOp::AcceptRes => {}
            TxnOp::CommitRes => {}
            TxnOp::Abort => {}
        }
    }

    // async fn run(&mut self) {
    //     loop {
    //         match self.recv.recv().await {
    //             Some(msg) =>
    //             None => continue,
    //         }
    //     }
    // }

    // async fn handle_read_only(&mut self, msg: Msg) {
    //     let txnid = msg.txn.txn_id;
    //     let txn = self.txns.remove(&txnid).unwrap();
    //     // execute
    //     let mut result = JanusMsg {
    //         txn_id: txnid,
    //         read_set: Vec::new(),
    //         write_set: Vec::new(),
    //         op: TxnOp::CommitRes.into(),
    //         from: self.server_id,
    //         deps: Vec::new(),
    //         txn_type: None,
    //     };

    //     for read in txn.read_set {
    //         let read_result = ReadStruct {
    //             key: read.key.clone(),
    //             value: Some(self.mem.get(&read.key).unwrap().read().await.1.clone()),
    //             timestamp: None,
    //         };
    //         result.read_set.push(read_result);
    //     }

    //     // reply to coordinator
    //     msg.callback.send(Ok(result)).await;
    // }

    async fn handle_execute(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        println!("execute txn {:?}", get_txnid(txnid));
        let txn = self.txns.remove(&txnid).unwrap();
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

        for read in txn.read_set {
            let read_result = ReadStruct {
                key: read.key.clone(),
                value: Some(self.mem.get(&read.key).unwrap().read().await.1.clone()),
                timestamp: None,
            };
            result.read_set.push(read_result);
        }

        for write in txn.write_set {
            self.mem.get(&write.key).unwrap().write().await.1 = write.value;
        }

        // reply to coordinator
        if msg.txn.from % 3 == self.server_id {
            msg.callback.send(Ok(result)).await;
        }
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        let mut result = JanusMsg {
            txn_id: msg.txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            deps: Vec::new(),
            txn_type: None,
        };
        // get the dep
        for read in msg.txn.read_set.iter() {
            let mut guard = self.mem.get(&read.key).unwrap().write().await;
            let dep = guard.0.last_visited_txnid;
            guard.0.last_visited_txnid = msg.txn.txn_id;
            result.deps.push(dep);
        }

        for write in msg.txn.write_set.iter() {
            let mut guard = self.mem.get(&write.key).unwrap().write().await;
            let dep = guard.0.last_visited_txnid;
            guard.0.last_visited_txnid = msg.txn.txn_id;
            result.deps.push(dep);
        }

        result.deps.sort();
        self.txns.insert(msg.txn.txn_id, msg.txn);
        // reply to coordinator
        msg.callback.send(Ok(result)).await;
        // println!("send back prepareok {:?}", result);
    }

    async fn handle_accept(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let accept_ok = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::AcceptRes.into(),
            from: self.server_id,
            deps: Vec::new(),
            txn_type: None,
        };
        // self.txns.insert(txnid, msg.txn);
        // reply accept ok to coordinator
        msg.callback.send(Ok(accept_ok)).await;
    }
}
