use common::PeerMsg;
use rpc::classic::Txn;
use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::http::header::EXPECT;

pub struct Dispatcher {
    //
    replica_id: i32,

    txn_id: u64,

    // sender to executor
    execs_senders: HashMap<i32, UnboundedSender<Txn>>,
    // sender to mpaxos leader

    // recv msgs from network
    rcv: UnboundedReceiver<PeerMsg>,
}

impl Dispatcher {
    pub async fn run_dispatcher(&mut self) {
        loop {
            if let Some(msg) = self.rcv.recv().await {
                match msg {
                    PeerMsg::Txn(txns) => {
                        for txn in txns.txns {
                            self.execs_senders.get(&txn.executor_id).unwrap().send(txn);
                        }
                    }
                    PeerMsg::MPaxos(mpaxos_msg) => {
                        // send to mpaxos leader
                    }
                }
            }
        }
    }
}
