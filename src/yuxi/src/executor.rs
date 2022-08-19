use std::{collections::HashMap, sync::Arc};

use common::tatp::{AccessInfo, CallForwarding, Subscriber};
use rpc::{
    common::{ReadStruct, TxnOp},
    yuxi::YuxiMsg,
};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{peer::IN_MEMORY_DATA, ExecuteContext, MaxTs, Msg, TS};

pub struct Executor {
    id: i32,
    server_id: i32,

    recv: UnboundedReceiver<Msg>,
    txns: HashMap<i64, (YuxiMsg, Vec<TS>)>,
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
        self.handle_commit(msg).await;
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
                let mut waitlist_guard = IN_MEMORY_DATA[*index].1.write().await;
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
                // let execute_context = ExecuteContext {
                //     committed: false,
                //     value: write.value.clone(),
                // };
                // waitlist_guard.insert(*guard, execute_context);
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
        let (txn, write_ts) = self.txns.remove(&tid).unwrap();

        // for read in msg.tmsg.read_set.iter() {
        //     let key = read.key;
        //     // find and erase the ts
        //     let mut guard = self.mem.get(&key).unwrap().write().await;
        // }
        unsafe {
            for i in 0..write_ts.len() {}
            for write in txn.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].1.write().await;
                // erase the ts from the wait list
            }
        }
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // commit final version and execute
        let tid = msg.tmsg.txn_id;
        let final_ts = msg.tmsg.timestamp;
        let (txn, write_ts) = self.txns.remove(&tid).unwrap();
        unsafe {
            for write in txn.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let tuple = &IN_MEMORY_DATA[*index];
                {
                    let mut ts = tuple.0.write().await;
                    if *ts < final_ts {
                        *ts = final_ts
                    }
                }
                // modify the wait list
                let mut wait_list = IN_MEMORY_DATA[*index].1.write().await;

                // check pending txns execute the context if the write is committed
            }

            let mut waiting_for_read_result = 0;
            let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
            let mut read_set = txn.read_set.clone();
            txn.read_set.clear();
            for read in read_set.iter_mut() {
                let key = read.key;
                let index = self.index.get(&key).unwrap();

                let tuple = &IN_MEMORY_DATA[*index];
                {
                    let mut ts = tuple.0.write().await;
                    if *ts < final_ts {
                        *ts = final_ts
                    }
                }

                let version_data = &tuple.2;
                let mut index = version_data.len() - 1;
                while final_ts < version_data[index].start_ts {
                    index -= 1;
                }
                if version_data[index].end_ts > final_ts {
                    // safe to read
                    read.value = Some(version_data[index].data.read());
                    txn.read_set.push(read.clone());
                } else {
                    // wait for the write
                    // let read_context = ExecuteContext {
                    //     read: true,
                    //     value: None,
                    //     call_back: Some(sender.clone()),
                    // };
                    let mut wait_list = tuple.1.write().await;
                    // if wait_list
                    // insert into
                    wait_list.insert(final_ts, read_context);
                    waiting_for_read_result += 1;
                }
            }
            if waiting_for_read_result == 0 {
                // reply to coordinator
                msg.callback.send(txn).await;
            } else {
                // spawn a new task for this
                tokio::spawn(async move {
                    while waiting_for_read_result > 0 {
                        let (key, value) = receiver.recv().await.unwrap();
                        txn.read_set.push(ReadStruct {
                            key,
                            value: Some(value),
                            timestamp: None,
                        });
                        waiting_for_read_result -= 1;
                    }
                    msg.callback.send(txn).await;
                });
            }
        }
    }
}
