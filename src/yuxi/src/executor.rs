use std::{collections::HashMap, sync::Arc};

use common::tatp::{AccessInfo, CallForwarding, Subscriber};
use rpc::{common::TxnOp, yuxi::YuxiMsg};
use tokio::sync::{mpsc::UnboundedReceiver, RwLock};

use crate::Msg;

pub struct Executor {
    id: i32,
    server_id: i32,

    recv: UnboundedReceiver<Msg>,

    // ycsb
    mem: Arc<HashMap<i64, RwLock<(u64, String)>>>,
    // tatp
    subscriber: Arc<HashMap<u64, Subscriber>>,
    access_info: Arc<HashMap<u64, AccessInfo>>,
    special_facility: Arc<HashMap<u64, AccessInfo>>,
    call_forwarding: Arc<HashMap<u64, CallForwarding>>,
}

impl Executor {
    pub fn new_ycsb() {}

    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => match msg.tmsg.op() {
                    TxnOp::Abort => self.handle_abort(msg).await,
                    TxnOp::ReadOnly => self.handle_read_only(msg).await,
                    TxnOp::Commit => self.handle_commit(msg).await,
                    TxnOp::Prepare => self.handle_prepare(msg).await,
                    TxnOp::PrepareRes => {}
                    TxnOp::ReadOnlyRes => todo!(),
                    TxnOp::Accept => self.handle_accept(msg).await,
                    TxnOp::AcceptRes => todo!(),
                    TxnOp::CommitRes => todo!(),
                },
                None => todo!(),
            }
        }
    }

    async fn handle_read_only(&mut self, msg: Msg) {
        // just wait for the earlier txn to be executed
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        //
        let mut prepare_response = YuxiMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.id,
            timestamp: msg.tmsg.timestamp,
            txn_type: None,
        };

        let ts = msg.tmsg.timestamp;

        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            // find and update the ts
            let mut guard = self.mem.get(&key).unwrap().write().await;
            if ts > guard.0 {
                guard.0 = ts;
            } else {
                guard.0 += 1;
                if prepare_response.timestamp < guard.0 {
                    prepare_response.timestamp = guard.0;
                }
            }
        }

        for write in msg.tmsg.write_set.iter() {
            let key = write.key;
            let mut guard = self.mem.get(&key).unwrap().write().await;
            if ts > guard.0 {
                guard.0 = ts;
            } else {
                guard.0 += 1;
                if prepare_response.timestamp < guard.0 {
                    prepare_response.timestamp = guard.0;
                }
            }
        }

        // send back result
        msg.callback.send(prepare_response).await;
    }

    async fn handle_accept(&mut self, msg: Msg) {
        // update final version
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // commit final version and execute
    }

    async fn handle_abort(&mut self, msg: Msg) {
        // abort the transaction
    }
}
