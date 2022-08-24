use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use chrono::Local;
use common::{config::Config, Data};
use rpc::{
    common::TxnOp,
    dast::{dast_client::DastClient, DastMsg},
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    time::sleep,
};
use tonic::transport::Channel;

use crate::{Msg, ClientMsg};

struct TxnInMemory {
    txn: DastMsg,
    callback: Option<Sender<DastMsg>>,
    reply_num: i32,
    committed: bool,
}

impl TxnInMemory {
    pub fn new_with_callback(txn: DastMsg, callback: Sender<DastMsg>) -> Self {
        Self {
            txn,
            callback: Some(callback),
            reply_num: 0,
            committed: false,
        }
    }

    pub fn new(txn: DastMsg) -> Self {
        Self {
            txn,
            callback: None,
            reply_num: 0,
            committed: false,
        }
    }
}

pub struct Peer {
    id: i32,
    readyq: BTreeMap<u64, Option<TxnInMemory>>,
    waitq: BTreeMap<u64, DastMsg>,
    notifiedTs: Vec<u64>,
    maxTs: Vec<u64>,
    // mytxns: HashMap<u64, TxnInMemory>,
    majority_size: i32,

    executed_up_to: u64,

    config: Config,
    manager_ids: Vec<i32>,
    local_node_ids: Vec<i32>,
    // rpc
    peer_senders: HashMap<i32, DastClient<Channel>>,

    recv: Receiver<DastMsg>,
}

impl Peer {
    pub fn new(id: i32, replica_nums: usize, config: Config) -> Self {
        return Self {
            id,
            readyq: BTreeMap::new(),
            waitq: BTreeMap::new(),

            majority_size: (config.server_ids.len() / 2) as i32,
            notifiedTs: vec![0; replica_nums],
            // mytxns: HashMap::new(),
            config,
            peer_senders: todo!(),
            recv: todo!(),
            executed_up_to: 0,
            maxTs: vec![0; replica_nums],
        };
    }

    pub fn create_ts(&mut self) -> u64 {
        let time = (Local::now().timestamp_nanos() / 1000) as u64;

        return time << 12 + self.id << 5;
    }

    pub async fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::PeerMsg(peer_msg) => {
                match peer_msg.op() {
                    TxnOp::ReadOnly => todo!(),
                    TxnOp::Prepare => self.handle_prepare(peer_msg).await,
                    TxnOp::Accept => todo!(),
                    TxnOp::Commit => todo!(),
                    TxnOp::ReadOnlyRes => todo!(),
                    TxnOp::PrepareRes => todo!(),
                    TxnOp::AcceptRes => todo!(),
                    TxnOp::CommitRes => todo!(),
                    TxnOp::Abort => todo!(),
                }
            },
            Msg::ClientMsg(client_msg) => self.coordIRT(client_msg).await,
        }
        
    }

    async fn coordIRT(&mut self, msg: ClientMsg) {
        let mut txn = msg.tmsg;
        let ts = self.create_ts();
        txn.timestamp = ts;
        let txn_in_memory = TxnInMemory::new_with_callback(txn.clone(), msg.callback);
        self.readyq.insert(ts, Some(txn_in_memory));
        // self.mytxns.insert(ts, txn_in_memory);
        // update notified ts

        // update maxts
        self.maxTs[self.id as usize] = ts;
        self.broadcast(txn).await;
    }

    async fn handle_prepare(&mut self, msg: DastMsg) {
        if msg.timestamp > self.maxTs[msg.from as usize] {
            self.maxTs[msg.from as usize] = msg.timestamp;
        }
        // insert ts into readyq
        for ts in msg.txn_ts.iter() {
            if *ts > self.executed_up_to && !self.readyq.contains_key(ts) {
                self.readyq.insert(*ts, None);
            }
        }
        //reply ack
        let ack = DastMsg{
            txn_id: msg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            notified_txn_ts: ,
            op: TxnOp::PrepareRes.into(),
            from: self.id,
            timestamp: msg.timestamp,
            txn_type: msg.txn_type,
        };
        // update notified ts
        
    }

    async fn handle_irt_ack(&mut self, txn: DastMsg) {
        let from = txn.from as usize;
        if txn.timestamp > self.notifiedTs[from] {
            self.notifiedTs[from] = txn.timestamp;
        }

        // insert ts into readyq
        for ts in txn.txn_ts.iter() {
            if *ts > self.executed_up_to && !self.readyq.contains_key(ts) {
                self.readyq.insert(*ts, None);
            }
        }

        let mut txn_in_memory = self
            .readyq
            .get_mut(&txn.timestamp)
            .unwrap()
            .as_mut()
            .unwrap();
        txn_in_memory.reply_num += 1;
        if txn_in_memory.reply_num == self.majority_size {
            // commit
            let commit = DastMsg {
                txn_id: txn.txn_id,
                read_set: Vec::new(),
                write_set: Vec::new(),
                op: TxnOp::Commit.into(),
                from: self.id,
                timestamp: txn.timestamp,
                txn_type: txn.txn_type,
                txn_ts: todo!(),
            };

            self.broadcast(commit).await;
        }
    }

    fn check_txn(&mut self) {
        for (ts, q) in self.readyq.iter() {
            match q {
                Some(txn_in_memory) => {
                    if txn_in_memory.committed {
                        // check
                        let mut safe = true;
                        for max_ts in self.maxTs.iter() {
                            if *max_ts < *ts {
                                safe = false;
                                break;
                            }
                        }
                        // execute
                        if safe {
                            //
                        }
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    async fn broadcast(&mut self, msg: DastMsg) {
        for (id, client) in self.peer_senders.iter() {
            client.txn_msg(request)
        }
    }

    async fn broadcast_to_managers(&mut self, msg: DastMsg) {}

    async fn broadcast_to_local_nodes(&mut self, msg: DastMsg) {}

    async fn send_to_peer(&mut self, msg: DastMsg) {}

    async fn init_rpc(&mut self) {
        // hold the clients to all the server
        for (id, server_addr) in self.config.server_addrs.iter() {
            println!("connect to {}-{}", id, server_addr);
            loop {
                match DastClient::connect(server_addr.clone()).await {
                    Ok(client) => {
                        self.peer_senders.insert(*id, client);
                        break;
                    }
                    Err(e) => {
                        println!("{}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}
