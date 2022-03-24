use std::collections::HashMap;

use common::{transaction_to_pieces, PeerMsg};
use rpc::classic::Txn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct TwoPLCoordinator {
    replica_id: i32,
    id: i32,
    txn_id: u64,
    // send to shards
    shard_senders: HashMap<i32, UnboundedSender<Txn>>,
    // recv msg from dispatcher
    receiver: UnboundedReceiver<Txn>,
}

impl TwoPLCoordinator {
    pub fn new(replica_id: i32, id: i32, receiver: UnboundedReceiver<Txn>) -> Self {
        let txn_id = (replica_id as u64) << 60 + (id as u64) << 55;
        Self {
            replica_id,
            id,
            txn_id,
            shard_senders: HashMap::new(),
            receiver,
        }
    }

    pub async fn handle(&mut self) {
        loop {
            match self.receiver.recv().await {
                Some(txn) => todo!(),
                None => {}
            }
        }
    }

    // get a tid
    pub fn begin_txn(&mut self, txn: &mut Txn) {
        let tid = self.txn_id + 1;
        self.txn_id += 1;
    }

    pub fn prepare_txn(&mut self, txn: Txn) {
        // shard the txns into pieces & send to peer
        let pieces = transaction_to_pieces(txn);
        for (shard, piece) in pieces.iter() {
            // self.prepare_txn(txn)
        }
    }

    pub fn commit_txn(&mut self, txn: Txn) {}

    pub fn abort_txn(&mut self, txn: Txn) {
        // send to shard to abort the txn
        // for (shard, piece)
    }

    pub fn send_to_shard(&mut self, shard_id: i32, txn: Txn) {
        let sender = self.shard_senders.get_mut(&shard_id).unwrap();
        sender.send(txn);
    }
}
