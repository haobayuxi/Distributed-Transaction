use common::config::MPaxosConfig;
use rpc::{
    classic::Txn,
    janus_rpc::TxnType,
    mpaxos::{MPaxosMsg, MPaxosMsgType},
};
use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub static mut TXNS: Vec<i64> = Vec::new();

pub enum Role {
    Leader,
    Follower,
}

pub struct Peer {
    role: Role,
    id: i32,
    ballot: i32,
    quorum_size: i32,
    log_index: i32,
    peers: HashMap<i32, UnboundedSender<MPaxosMsg>>,

    // got msg from txn executor
    receiver: UnboundedReceiver<Txn>,
    // the sender send the txns received to txn instance
    to_txn_instance: UnboundedSender<Txn>,
}
impl Peer {
    // pub fn new(id: i32, config: MPaxosConfig)->Self{
    //     Self{

    //     }
    // }

    pub fn handle_msg(&mut self, msg: MPaxosMsg) {
        match msg.msg_type() {
            MPaxosMsgType::Set => self.handle_leader_put(msg),
            MPaxosMsgType::SetResponse => todo!(),
        }
    }

    pub fn handle_client_request(&mut self, txns: Txn) {
        let index = self.log_index;
        self.log_index += 1;
        let msg = MPaxosMsg {
            from: self.id,
            ballot: self.ballot,
            index,
            txns: Some(txns),
            msg_type: MPaxosMsgType::Set.into(),
        };

        self.broadcast_to_peer(msg);
    }

    fn handle_leader_put(&mut self, msg: MPaxosMsg) {
        if self.ballot == msg.ballot {
            self.to_txn_instance.send(msg.txns.unwrap());
            // reply to leader
            let reply = MPaxosMsg {
                msg_type: MPaxosMsgType::SetResponse.into(),
                from: self.id,
                ballot: self.ballot,
                index: msg.index,
                txns: Option::None,
            };
            self.send_to_peer(msg.from, reply);
        }
    }

    fn broadcast_to_peer(&mut self, msg: MPaxosMsg) {
        for (_id, sender) in self.peers.iter() {
            sender.send(msg.clone());
        }
    }

    fn send_to_peer(&mut self, to: i32, msg: MPaxosMsg) {
        self.peers.get(&to).unwrap().send(msg);
    }

    fn send_to_executor(&mut self) {}

    async fn run(&mut self) {
        loop {
            match self.receiver.recv().await {
                Some(txn) => todo!(),
                None => todo!(),
            }
        }
    }
}
