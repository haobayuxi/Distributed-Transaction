use rpc::{classic::Txn, multipaxos_rpc::MPaxosMsg};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

pub static mut TXNS: Vec<i64> = Vec::new();

pub enum Role {
    Leader,
    Follower,
}

pub struct Peer {
    role: Role,
    id: i32,
    ballot: i32,
    log_index: i32,
    peers: HashMap<i32, UnboundedSender<MPaxosMsg>>,

    // the sender send the txns received to txn instance
    to_txn_instance: UnboundedSender<Txn>,
}
impl Peer {
    // pub fn new(config)->Self{
    //     Self{

    //     }
    // }

    pub fn put(&mut self, txns: Txn) {
        let index = self.log_index;
        self.log_index += 1;
        let msg = MPaxosMsg {
            from: self.id,
            ballot: self.ballot,
            index,
            txns: Some(txns),
        };

        self.broadcast_to_peer(msg);
    }

    fn handle_leader_put(&mut self, msg: MPaxosMsg) {
        if self.ballot == msg.ballot {
            self.to_txn_instance.send(msg.txns.unwrap());
        }
    }

    fn broadcast_to_peer(&mut self, msg: MPaxosMsg) {
        for (_id, sender) in self.peers.iter() {
            sender.send(msg.clone());
        }
    }

    async fn init_peer_rpc(&mut self) {}
}
