use std::{collections::HashMap, sync::Arc};

use rpc::{
    common::ReadStruct,
    janus::{JanusMsg, TxnOp},
};
use tokio::sync::{mpsc::UnboundedReceiver, RwLock};

use crate::{JanusMeta, Msg};

pub struct Executor {
    id: i32,
    server_id: i32,
    // memory
    mem: Arc<HashMap<i64, RwLock<(JanusMeta, String)>>>,
    // txns
    txns: HashMap<i64, JanusMsg>,
    //
    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    fn new(
        id: i32,
        server_id: i32,
        mem: Arc<HashMap<i64, RwLock<(JanusMeta, String)>>>,
        recv: UnboundedReceiver<Msg>,
    ) -> Self {
        Self {
            id,
            server_id,
            mem,
            txns: HashMap::new(),
            recv,
        }
    }

    async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => match msg.txn.op() {
                    TxnOp::ReadOnly => self.handle_read_only(msg).await,
                    TxnOp::Prepare => self.handle_prepare(msg).await,
                    TxnOp::Accept => self.handle_accept(msg),
                    TxnOp::Commit => self.handle_execute(msg).await,
                    TxnOp::ReadOnlyRes => todo!(),
                    TxnOp::PrepareRes => todo!(),
                    TxnOp::AcceptRes => todo!(),
                    TxnOp::CommitRes => todo!(),
                },
                None => continue,
            }
        }
    }

    async fn handle_read_only(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let txn = self.txns.remove(&txnid).unwrap();
        // execute
        let mut result = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_ids: Vec::new(),
            op: TxnOp::CommitRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };

        for read in txn.read_set {
            let read_result = ReadStruct {
                key: read.key.clone(),
                value: Some(self.mem.get(&read.key).unwrap().read().await.1.clone()),
            };
            result.read_set.push(read_result);
        }

        // reply to coordinator
        msg.callback.send(result);
    }

    async fn handle_execute(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let txn = self.txns.remove(&txnid).unwrap();
        // execute
        let mut result = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_ids: Vec::new(),
            op: TxnOp::CommitRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };

        for read in txn.read_set {
            let read_result = ReadStruct {
                key: read.key.clone(),
                value: Some(self.mem.get(&read.key).unwrap().read().await.1.clone()),
            };
            result.read_set.push(read_result);
        }

        for write in txn.write_set {
            self.mem.get(&write.key).unwrap().write().await.1 = write.value;
        }

        // reply to coordinator
        msg.callback.send(result);
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        let mut result = JanusMsg {
            txn_id: msg.txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_ids: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            deps: Vec::new(),
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

        self.txns.insert(msg.txn.txn_id, msg.txn);
        // reply to coordinator
        msg.callback.send(result);
    }

    fn handle_accept(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let accept_ok = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_ids: Vec::new(),
            op: TxnOp::AcceptRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };
        // self.txns.insert(txnid, msg.txn);
        // reply accept ok to coordinator
        msg.callback.send(accept_ok);
    }
}
