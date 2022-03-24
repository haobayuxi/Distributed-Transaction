use std::{collections::HashMap, sync::Arc};

use rpc::classic::{Txn, TxnOp};
use tokio::sync::{mpsc::UnboundedReceiver, OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

use crate::memory::Memory;

#[derive(Default)]
struct TxnLock {
    read_lock: Vec<OwnedRwLockReadGuard<String>>,
    write_lock: Vec<OwnedRwLockWriteGuard<String>>,
}

pub struct Executor {
    pub executor_id: i32,
    locks: HashMap<i64, TxnLock>,
    // task receiver
    rx: UnboundedReceiver<Txn>,
    // memory
    mem: Arc<Memory>,
}

impl Executor {
    pub fn new() {}

    pub fn handle_txn_req(&mut self) {}

    pub fn handle_prepare(&mut self, txn: Txn) {
        let mut locks = TxnLock::default();
        // lock the read set
        for read in txn.read_set.iter() {
            match self
                .mem
                .data
                .get(&read.key)
                .unwrap()
                .clone()
                .try_read_owned()
            {
                Ok(guard) => {
                    locks.read_lock.push(guard);
                }
                Err(_) => {
                    // lock fail, return to coordinator
                    let mut abort = Txn::default();
                    abort.op = TxnOp::Abort.into();
                    abort.txn_id = txn.txn_id;

                    return;
                }
            }
        }
        // lock the write set
    }

    pub fn handle_abort(&mut self, txn: Txn) {
        // release the lock
    }

    pub fn handle_commit(&mut self, txn: Txn) {
        // update the tuple

        // release the lock
    }
}
