use std::{collections::HashMap, sync::Arc};

use common::{
    get_txnid,
    tatp::{AccessInfo, CallForwarding, Subscriber},
};
use rpc::{
    common::{ReadStruct, TxnOp},
    meerkat::MeerkatMsg,
};
use tokio::sync::{mpsc::UnboundedReceiver, OwnedRwLockWriteGuard, RwLock};

use crate::{MeerkatMeta, Msg};

pub struct Executor {
    id: u32,
    server_id: u32,
    // ycsb
    mem: Arc<HashMap<i64, RwLock<(MeerkatMeta, String)>>>,
    // tatp
    subscriber: Arc<HashMap<u64, Subscriber>>,
    access_info: Arc<HashMap<u64, AccessInfo>>,
    special_facility: Arc<HashMap<u64, AccessInfo>>,
    call_forwarding: Arc<HashMap<u64, CallForwarding>>,
    // write lock guard
    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new_ycsb(
        id: u32,
        server_id: u32,
        mem: Arc<HashMap<i64, RwLock<(MeerkatMeta, String)>>>,
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
                    TxnOp::Abort => self.handle_abort(msg).await,
                    TxnOp::ReadOnly => self.handle_read(msg).await,
                    TxnOp::Commit => self.handle_commit(msg).await,
                    TxnOp::Prepare => self.handle_prepare(msg).await,
                    TxnOp::PrepareRes => {}
                    TxnOp::ReadOnlyRes => {}
                    TxnOp::Accept => {}
                    TxnOp::AcceptRes => {}
                    TxnOp::CommitRes => {}
                },
                None => {}
            }
        }
    }

    async fn handle_read(&mut self, msg: Msg) {
        // perform read & return read ts
        let mem = self.mem.clone();
        let server_id = self.server_id;
        // tokio::spawn(async move {
        let mut result_read_set = Vec::new();
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let read_guard = mem.get(&key).unwrap().read().await;
            let result = ReadStruct {
                key,
                value: Some(read_guard.1.clone()),
                timestamp: Some(read_guard.0.version),
            };
            result_read_set.push(result);
        }
        // send result back
        let read_back = MeerkatMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: result_read_set,
            write_set: Vec::new(),
            executor_id: 0,
            op: TxnOp::ReadOnlyRes.into(),
            from: server_id,
            timestamp: 0,
            txn_type: None,
        };
        msg.callback.send(Ok(read_back)).await.unwrap();
        // });
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        println!("handle prepare {:?}", get_txnid(msg.tmsg.txn_id));
        // check read set
        for read in msg.tmsg.read_set.iter() {
            // match self.mem.get(&read.key).unwrap().try_read() {
            //     Ok(read_guard) => {}
            //     Err(_) => {
            //         // abort the txn
            //         let abort_msg = MeerkatMsg {
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
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write().await;
            if read.timestamp.unwrap() < guard.0.version
                || (guard.0.prepared_write.len() > 0
                    && read.timestamp.unwrap() < *guard.0.prepared_write.iter().min().unwrap())
            {
                // abort the txn
                let abort_msg = MeerkatMsg {
                    txn_id: msg.tmsg.txn_id,
                    read_set: Vec::new(),
                    write_set: Vec::new(),
                    executor_id: self.id,
                    op: TxnOp::Abort.into(),
                    from: self.server_id,
                    timestamp: 0,
                    txn_type: None,
                };
                // send back to client
                msg.callback.send(Ok(abort_msg)).await.unwrap();
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
            //         let abort_msg = MeerkatMsg {
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
                    && msg.tmsg.timestamp < *guard.0.prepared_read.iter().last().unwrap())
            {
                // abort the txn
                let abort_msg = MeerkatMsg {
                    txn_id: msg.tmsg.txn_id,
                    read_set: Vec::new(),
                    write_set: Vec::new(),
                    executor_id: self.id,
                    op: TxnOp::Abort.into(),
                    from: self.server_id,
                    timestamp: 0,
                    txn_type: None,
                };
                // send back to client
                msg.callback.send(Ok(abort_msg)).await;
                return;
            }
            guard.0.prepared_write.insert(msg.tmsg.timestamp);
        }
        // return prepare ok
        let prepare_ok = MeerkatMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            timestamp: 0,
            txn_type: None,
        };
        msg.callback.send(Ok(prepare_ok)).await.unwrap();
        // println!("send prepareok back");
    }

    async fn handle_commit(&mut self, msg: Msg) {
        println!("handle commit {:?}", get_txnid(msg.tmsg.txn_id));
        // update
        // release the prepare  read & prepare write
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write().await;
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
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write().await;
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
