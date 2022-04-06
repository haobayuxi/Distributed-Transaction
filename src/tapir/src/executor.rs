use std::{collections::HashMap, sync::Arc};

use common::tatp::{AccessInfo, CallForwarding, Subscriber};
use rpc::tapir::{ReadStruct, TapirMsg, TxnOp, WriteStruct};
use tokio::sync::{mpsc::UnboundedReceiver, OwnedRwLockWriteGuard, RwLock};

use crate::{Msg, TapirMeta};

pub struct Executor {
    id: i32,
    server_id: i32,
    // ycsb
    mem: Arc<HashMap<i64, RwLock<(TapirMeta, String)>>>,
    // tatp
    subscriber: Arc<HashMap<u64, Subscriber>>,
    access_info: Arc<HashMap<u64, AccessInfo>>,
    special_facility: Arc<HashMap<u64, AccessInfo>>,
    call_forwarding: Arc<HashMap<u64, CallForwarding>>,
    // write lock guard
    // guards: HashMap<String, OwnedRwLockWriteGuard<(i64, String)>>,
    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new_ycsb(
        id: i32,
        server_id: i32,
        mem: Arc<HashMap<i64, RwLock<(TapirMeta, String)>>>,
        recv: UnboundedReceiver<Msg>,
    ) -> Self {
        Self {
            id,
            server_id,
            mem,
            // guards: HashMap::new(),
            recv,
            subscriber: Arc::new(HashMap::new()),
            access_info: Arc::new(HashMap::new()),
            special_facility: Arc::new(HashMap::new()),
            call_forwarding: Arc::new(HashMap::new()),
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => match msg.tmsg.op() {
                    TxnOp::TAbort => self.handle_abort(msg).await,
                    TxnOp::TRead => self.handle_read(msg).await,
                    TxnOp::TCommit => self.handle_commit(msg).await,
                    TxnOp::TPrepare => self.handle_prepare(msg).await,
                    TxnOp::TPrepareOk => {}
                    TxnOp::TReadRes => todo!(),
                },
                None => todo!(),
            }
        }
    }

    async fn handle_read(&mut self, msg: Msg) {
        // perform read & return read ts
        let mem = self.mem.clone();
        let server_id = self.server_id;
        tokio::spawn(async move {
            let mut result_read_set = Vec::new();
            for read in msg.tmsg.read_set.iter() {
                // let read_guard = mem.get(&read.key).unwrap().read().await;
                let read_guard = mem.get(&read.key).unwrap().read().await;
                let result = ReadStruct {
                    key: read.key.clone(),
                    value: Some(read_guard.1.clone()),
                    timestamp: Some(read_guard.0.version),
                };
                result_read_set.push(result);
            }
            // send result back
            let read_back = TapirMsg {
                txn_id: msg.tmsg.txn_id,
                read_set: result_read_set,
                write_set: Vec::new(),
                executor_id: 0,
                op: TxnOp::TReadRes.into(),
                from: server_id,
                timestamp: 0,
                txn_type: None,
            };
            msg.callback.send(read_back).await;
        });
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        // check read set
        for read in msg.tmsg.read_set.iter() {
            // match self.mem.get(&read.key).unwrap().try_read() {
            //     Ok(read_guard) => {}
            //     Err(_) => {
            //         // abort the txn
            //         let abort_msg = TapirMsg {
            //             txn_id: msg.tmsg.txn_id,
            //             read_set: Vec::new(),
            //             write_set: Vec::new(),
            //             executor_id: self.id,
            //             op: TxnOp::TAbort.into(),
            //             from: self.server_id,
            //             timestamp: 0,
            //         };
            //         // send back to client
            //         msg.callback.send(abort_msg).await;
            //         return;
            //     }
            // }
            let mut guard = self.mem.get(&read.key).unwrap().write().await;
            if read.timestamp.unwrap() < guard.0.version
                || (guard.0.prepared_write.len() > 0
                    && read.timestamp.unwrap() < *guard.0.prepared_write.first().unwrap())
            {
                // abort the txn
                let abort_msg = TapirMsg {
                    txn_id: msg.tmsg.txn_id,
                    read_set: Vec::new(),
                    write_set: Vec::new(),
                    executor_id: self.id,
                    op: TxnOp::TAbort.into(),
                    from: self.server_id,
                    timestamp: 0,
                    txn_type: None,
                };
                // send back to client
                msg.callback.send(abort_msg).await;
                return;
            }
            // insert ts to prepared read
            guard.0.prepared_read.insert(msg.tmsg.timestamp);
        }
        // validate write set
        for write in msg.tmsg.write_set.iter() {
            // match self.mem.get(&write.key).unwrap().clone().try_write_owned() {
            //     Ok(write_gurad) => {
            //         self.guards.insert(write.key.clone(), write_gurad);
            //     }
            //     Err(_) => {
            //         // abort
            //         let abort_msg = TapirMsg {
            //             txn_id: msg.tmsg.txn_id,
            //             read_set: Vec::new(),
            //             write_set: Vec::new(),
            //             executor_id: self.id,
            //             op: TxnOp::TAbort.into(),
            //             from: self.server_id,
            //             timestamp: 0,
            //         };
            //         // send back to client
            //         msg.callback.send(abort_msg).await;
            //         return;
            //     }
            // }
            let mut guard = self.mem.get(&write.key).unwrap().write().await;
            if msg.tmsg.timestamp < guard.0.version
                || (guard.0.prepared_read.len() > 0
                    && msg.tmsg.timestamp < *guard.0.prepared_read.last().unwrap())
            {
                // abort the txn
                let abort_msg = TapirMsg {
                    txn_id: msg.tmsg.txn_id,
                    read_set: Vec::new(),
                    write_set: Vec::new(),
                    executor_id: self.id,
                    op: TxnOp::TAbort.into(),
                    from: self.server_id,
                    timestamp: 0,
                    txn_type: None,
                };
                // send back to client
                msg.callback.send(abort_msg).await;
                return;
            }
            guard.0.prepared_write.insert(msg.tmsg.timestamp);
        }
        // return prepare ok
        let prepare_ok = TapirMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::TPrepareOk.into(),
            from: self.server_id,
            timestamp: 0,
            txn_type: None,
        };
        msg.callback.send(prepare_ok).await;
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // update
        // release the prepare  read & prepare write
        for read in msg.tmsg.read_set.iter() {
            let mut guard = self.mem.get(&read.key).unwrap().write().await;
            guard.0.prepared_read.remove(&msg.tmsg.timestamp);
        }

        for write in msg.tmsg.write_set {
            // let mut guard = self.guards.remove(&write.key).unwrap();
            // guard.0 = msg.tmsg.txn_id;
            // guard.1 = write.value.clone();
            // update value
            let mut guard = self.mem.get(&write.key).unwrap().write().await;
            guard.1 = write.value;
            guard.0.prepared_write.remove(&msg.tmsg.timestamp);
        }
    }

    async fn handle_abort(&mut self, msg: Msg) {
        // release the prepare  read & prepare write
        for read in msg.tmsg.read_set.iter() {
            let mut guard = self.mem.get(&read.key).unwrap().write().await;
            guard.0.prepared_read.remove(&msg.tmsg.timestamp);
        }

        for write in msg.tmsg.write_set {
            // let mut guard = self.guards.remove(&write.key).unwrap();
            // guard.0 = msg.tmsg.txn_id;
            // guard.1 = write.value.clone();
            // update value
            let mut guard = self.mem.get(&write.key).unwrap().write().await;
            guard.0.prepared_write.remove(&msg.tmsg.timestamp);
        }
    }
}
