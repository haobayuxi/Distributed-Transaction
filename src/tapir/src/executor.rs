use std::{collections::HashMap, sync::Arc};

use rpc::tapir::{ReadStruct, TapirMsg, TxnOp, WriteStruct};
use tokio::sync::{mpsc::UnboundedReceiver, OwnedRwLockWriteGuard, RwLock};

use crate::Msg;

pub struct Executor {
    id: i32,
    server_id: i32,
    mem: Arc<HashMap<String, Arc<RwLock<(i64, String)>>>>,
    // write lock guard
    guards: HashMap<String, OwnedRwLockWriteGuard<(i64, String)>>,
    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new(
        id: i32,
        server_id: i32,
        mem: Arc<HashMap<String, Arc<RwLock<(i64, String)>>>>,
        recv: UnboundedReceiver<Msg>,
    ) -> Self {
        Self {
            id,
            server_id,
            mem,
            guards: HashMap::new(),
            recv,
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => match msg.tmsg.op() {
                    TxnOp::TAbort => self.handle_abort(msg),
                    TxnOp::TRead => self.handle_read(msg).await,
                    TxnOp::TCommit => self.handle_commit(msg),
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
                let read_guard = mem.get(&read.key).unwrap().read().await;
                let result = ReadStruct {
                    key: read.key.clone(),
                    value: Some(read_guard.1.clone()),
                    timestamp: Some(read_guard.0),
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
            };
            msg.callback.send(read_back).await;
        });
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        // check read set
        for read in msg.tmsg.read_set.iter() {
            match self.mem.get(&read.key).unwrap().try_read() {
                Ok(read_guard) => todo!(),
                Err(_) => {
                    // abort the txn
                    let abort_msg = TapirMsg {
                        txn_id: msg.tmsg.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        executor_id: self.id,
                        op: TxnOp::TAbort.into(),
                        from: self.server_id,
                    };
                    // send back to client
                    msg.callback.send(abort_msg).await;
                    return;
                }
            }
        }
        // lock write set
        for write in msg.tmsg.write_set.iter() {
            match self.mem.get(&write.key).unwrap().clone().try_write_owned() {
                Ok(write_gurad) => {
                    self.guards.insert(write.key.clone(), write_gurad);
                }
                Err(_) => {
                    // abort
                    let abort_msg = TapirMsg {
                        txn_id: msg.tmsg.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        executor_id: self.id,
                        op: TxnOp::TAbort.into(),
                        from: self.server_id,
                    };
                    // send back to client
                    msg.callback.send(abort_msg).await;
                    return;
                }
            }
        }
        // return prepare ok
        let prepare_ok = TapirMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::TPrepareOk.into(),
            from: self.server_id,
        };
        msg.callback.send(prepare_ok).await;
    }

    fn handle_commit(&mut self, msg: Msg) {
        // release the write lock & update value
        for write in msg.tmsg.write_set.iter() {
            let mut guard = self.guards.remove(&write.key).unwrap();
            guard.0 = msg.tmsg.txn_id;
            guard.1 = write.value.clone();
        }
    }

    fn handle_abort(&mut self, msg: Msg) {
        // release the write lock
        for write in msg.tmsg.write_set.iter() {
            let _ = self.guards.remove(&write.key);
        }
    }
}
