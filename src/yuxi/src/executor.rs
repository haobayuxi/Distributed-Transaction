use std::{collections::HashMap, sync::Arc};

use common::tatp::{AccessInfo, CallForwarding, Subscriber};
use rpc::{common::TxnOp, yuxi::YuxiMsg};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{peer::IN_MEMORY_DATA, MaxTs, Msg};

struct ExecuteContext {
    read: bool,
    value: Option<String>,
    call_back: UnboundedSender<String>,
}
pub struct Executor {
    id: i32,
    server_id: i32,

    recv: UnboundedReceiver<Msg>,
    txns: HashMap<i64, YuxiMsg>,
    // ycsb
    index: Arc<HashMap<i64, usize>>,
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
        unsafe {
            for read in msg.tmsg.read_set.iter() {
                let key = read.key;
                // find and update the ts
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;
                if ts > *guard {
                    *guard = ts;
                } else {
                    if prepare_response.timestamp < *guard {
                        prepare_response.timestamp = *guard;
                    }
                }
            }

            for write in msg.tmsg.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;
                if ts > *guard {
                    *guard = ts;
                } else {
                    *guard += 1;
                    if prepare_response.timestamp < *guard {
                        prepare_response.timestamp = *guard;
                    }
                }
                // insert into wait list
            }
        }

        // send back result
        msg.callback.send(prepare_response).await;
    }

    async fn handle_accept(&mut self, msg: Msg) {
        // update final version

        // reply accept ok
        let result = YuxiMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::AcceptRes.into(),
            from: self.id,
            timestamp: 0,
            txn_type: None,
        };
        msg.callback.send(result).await;
    }

    async fn handle_abort(&mut self, msg: Msg) {
        // abort the transaction
        let tid = msg.tmsg.txn_id;
        let txn = self.txns.remove(&tid).unwrap();

        // for read in msg.tmsg.read_set.iter() {
        //     let key = read.key;
        //     // find and erase the ts
        //     let mut guard = self.mem.get(&key).unwrap().write().await;
        // }
        unsafe {
            for write in txn.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;
                // erase the ts from the wait list
            }
        }
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // commit final version and execute
        let tid = msg.tmsg.txn_id;
        let final_ts = msg.tmsg.timestamp;
        let mut txn = self.txns.remove(&tid).unwrap();
        txn.from = self.server_id;
        unsafe {
            for write in txn.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].1.write().await;
                // modify the wait list
                // let mut list = guard
                // check pending txns
            }

            let mut waiting_for_read_result = 0;
            let (sender, receiver) = unbounded_channel::<String>();
            for read in txn.read_set.iter_mut() {
                let key = read.key;
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;

                let tuple = &IN_MEMORY_DATA[*index];
                let version_data = &tuple.2;
                let mut index = version_data.len() - 1;
                while final_ts < version_data[index].start_ts {
                    index -= 1;
                }
                if version_data[index].end_ts > final_ts {
                    // safe to read
                } else {
                    // wait for the write
                    waiting_for_read_result += 1;
                }
            }
            if waiting_for_read_result == 0 {
                // reply to coordinator
            } else {
                // spawn a new task for this
                tokio::spawn(async move {
                    while waiting_for_read_result > 0 {
                        let result = receiver.recv().await;

                        waiting_for_read_result -= 1;
                    }
                    msg.callback.send(value).await;
                });
            }
        }
    }
}
