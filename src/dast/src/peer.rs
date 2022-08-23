use std::{collections::BTreeMap, sync::Arc};

use chrono::Local;
use rpc::{common::TxnOp, dast::DastMsg};
use tokio::sync::RwLock;

pub struct Peer {
    id: i32,
    readyq: BTreeMap<u64, DastMsg>,
    waitq: BTreeMap<u64, DastMsg>,
    notifiedTs: Vec<u64>,
    mytxns: BTreeMap<u64, DastMsg>,
}

impl Peer {
    pub fn new(id: i32, replica_nums: usize) -> Self {
        return Self {
            id,
            readyq: BTreeMap::new(),
            waitq: BTreeMap::new(),
            notifiedTs: vec![0; replica_nums],
            mytxns: BTreeMap::new(),
        };
    }

    pub fn create_ts(&mut self) -> u64 {
        let time = (Local::now().timestamp_nanos() / 1000) as u64;

        return time << 12 + self.id << 5;
    }

    async fn CoordIRT(&mut self, msg: DastMsg) {
        let mut txn = msg;
        let ts = self.create_ts();
        txn.timestamp = ts;
        self.readyq.insert(ts, txn.clone());
        self.mytxns.insert(ts, txn.clone());
        self.broadcast(txn).await;
    }

    async fn handle_irt_ack(&mut self, txn: DastMsg) {
        let from = txn.from as usize;
        if txn.timestamp > self.notifiedTs[from] {
            self.notifiedTs[from] = txn.timestamp;
        }

        let commit = DastMsg {
            txn_id: txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::Commit.into(),
            from: self.id,
            timestamp: txn.timestamp,
            txn_type: txn.txn_type,
        };

        self.broadcast(commit).await;
    }

    fn check_txn(&mut self) {}

    async fn broadcast(&mut self, msg: DastMsg) {}

    async fn send_to_peer(&mut self, msg: DastMsg) {}
}
