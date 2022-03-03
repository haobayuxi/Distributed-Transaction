use rpc::multipaxos_rpc::MPaxosMsg;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

pub enum Role {
    Leader,
    Follower,
}

pub struct Peer {
    role: Role,
    log_index: i32,
    peers: HashMap<i32, UnboundedSender<MPaxosMsg>>,
}
impl Peer {
    // pub fn new()->Self{
    //     Self{

    //     }
    // }

    pub async fn put(&mut self) {
        let index = self.log_index;
        self.log_index += 1;
    }

    fn broadcast_to_peer(&mut self, msg: MPaxosMsg) {
        for (_id, sender) in self.peers.iter() {
            sender.send(msg.clone());
        }
    }

    async fn init_peer_rpc(&mut self) {}
}
