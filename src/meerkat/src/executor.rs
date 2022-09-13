use std::{collections::HashMap, sync::Arc};

use common::{
    get_txnid,
    tatp::{AccessInfo, CallForwarding, Subscriber},
};
use parking_lot::RwLock;
use rpc::{
    common::{ReadStruct, TxnOp},
    meerkat::MeerkatMsg,
};
use tokio::sync::mpsc::UnboundedReceiver;

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
        // let mem = self.mem.clone();
        let server_id = self.server_id;
        let mut result_read_set = Vec::new();
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let read_guard = self.mem.get(&key).unwrap().read();
            let result = ReadStruct {
                key,
                value: Some(read_guard.1.clone()),
                timestamp: Some(read_guard.0.version),
            };
            result_read_set.push(result);
        }
        // send result back
        // println!("read set {:?}", result_read_set);
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
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        // println!("handle prepare {:?}", get_txnid(msg.tmsg.txn_id));
        // check read sets
        let mut abort = false;
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write();
            if read.timestamp() < guard.0.version
                || (guard.0.prepared_write.len() > 0
                    && msg.tmsg.timestamp < *guard.0.prepared_write.iter().min().unwrap())
            {
                abort = true;
                break;
            }
            // insert ts to prepared read
            guard.0.prepared_read.insert(msg.tmsg.timestamp);
        }
        if !abort {
            for write in msg.tmsg.write_set.iter() {
                let mut guard = self.mem.get(&write.key).unwrap().write();
                if msg.tmsg.timestamp < guard.0.rts
                    || (guard.0.prepared_read.len() > 0
                        && msg.tmsg.timestamp < *guard.0.prepared_read.iter().max().unwrap())
                {
                    // abort the txn
                    abort = true;
                    break;
                }
                guard.0.prepared_write.insert(msg.tmsg.timestamp);
            }
        }
        // return prepare ok
        let mut prepare_ok = MeerkatMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_id: self.id,
            op: TxnOp::Prepare.into(),
            from: self.server_id,
            timestamp: 0,
            txn_type: None,
        };
        // validate write set
        if abort {
            println!("abort the txn");
            prepare_ok.op = TxnOp::Abort.into();
        } else {
            prepare_ok.op = TxnOp::PrepareRes.into();
        }

        msg.callback.send(Ok(prepare_ok)).await.unwrap();
        // println!("send prepareok back");
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // println!("handle commit {:?}", get_txnid(msg.tmsg.txn_id));
        // update
        // release the prepare  read & prepare write
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write();
            guard.0.prepared_read.remove(&msg.tmsg.timestamp);
            if guard.0.rts < msg.tmsg.timestamp {
                guard.0.rts = msg.tmsg.timestamp;
            }
        }

        for write in msg.tmsg.write_set {
            // let mut guard = self.guards.remove(&write.key).unwrap();
            // guard.0 = msg.tmsg.txn_id;
            // guard.1 = write.value.clone();
            // update value
            let mut guard = self.mem.get(&write.key).unwrap().write();
            guard.1 = write.value;
            guard.0.prepared_write.remove(&msg.tmsg.timestamp);
            if guard.0.version < msg.tmsg.timestamp {
                guard.0.version = msg.tmsg.timestamp
            }
        }
    }

    async fn handle_abort(&mut self, msg: Msg) {
        // release the prepare  read & prepare write
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            let mut guard = self.mem.get(&key).unwrap().write();
            guard.0.prepared_read.remove(&msg.tmsg.timestamp);
        }

        for write in msg.tmsg.write_set {
            // let mut guard = self.guards.remove(&write.key).unwrap();
            // guard.0 = msg.tmsg.txn_id;
            // guard.1 = write.value.clone();
            // update value
            let mut guard = self.mem.get(&write.key).unwrap().write();
            guard.0.prepared_write.remove(&msg.tmsg.timestamp);
        }
    }
}
