use std::{collections::HashMap, sync::Arc};

use rpc::janus::{JanusMsg, TxnOp};
use tokio::sync::RwLock;

use crate::Msg;

pub struct Executor {
    id: i32,
    server_id: i32,
    // memory
    mem: Arc<HashMap<String, RwLock<String>>>,
    // txns
    txns: HashMap<i64, JanusMsg>,
}

impl Executor {
    fn handle_execute(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let txn = self.txns.remove(&txnid).unwrap();
        // execute
        let mut result = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::CommitRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };

        for read in txn.read_set {
            let readd = result.read_set.push();
        }

        for write in txn.write_set {}

        // reply to coordinator
        msg.callback.send(result);
    }

    fn handle_prepare(&mut self, msg: Msg) {
        let mut result = JanusMsg {
            txn_id: msg.txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };
        // get the dep
        for read in msg.txn.read_set {}
        // reply to coordinator
        msg.callback.send(result);
    }

    fn handle_accept(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let accept_ok = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::AcceptRes.into(),
            from: self.server_id,
            deps: Vec::new(),
        };
        self.txns.insert(txnid, msg.txn);
        // reply accept ok to coordinator
        msg.callback.send(accept_ok);
    }
}
