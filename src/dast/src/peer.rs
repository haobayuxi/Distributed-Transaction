use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use chrono::Local;
use common::{config::Config, convert_ip_addr, get_txnid, ycsb::init_ycsb};
use rpc::{common::TxnOp, dast::DastMsg};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{channel, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    time::sleep,
    time::Duration,
};
use tracing::info;

use crate::{
    peer_communication::{run_propose_server, run_rpc_server, ProposeServer, RpcClient, RpcServer},
    ClientMsg, Msg,
};

// pub static mut COMMITTED: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Debug)]
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
    id: u32,
    readyq: BTreeMap<u64, Option<TxnInMemory>>,
    waitq: BTreeMap<u64, DastMsg>,
    notifiedTs: Vec<u64>,
    maxTs: Vec<u64>,
    mytxns: BTreeMap<u64, TxnInMemory>,
    majority_size: i32,

    executed_up_to: u64,

    config: Config,
    manager_ids: Vec<u32>,
    local_node_ids: Vec<i32>,
    // rpc
    peer_senders: HashMap<u32, Sender<DastMsg>>,

    recv: UnboundedReceiver<Msg>,

    // data
    ycsb: HashMap<u64, String>,
    // executed: u64,
}

impl Peer {
    pub fn new(
        id: u32,
        replica_nums: usize,
        config: Config,
        recv: UnboundedReceiver<Msg>,
        is_ycsb: bool,
    ) -> Self {
        let mut ycsb = HashMap::new();
        if is_ycsb {
            ycsb = init_ycsb();
        } else {
        }
        return Self {
            id,
            readyq: BTreeMap::new(),
            waitq: BTreeMap::new(),

            majority_size: 2,
            notifiedTs: vec![0; replica_nums],

            manager_ids: config.manager_ids.clone(),
            config,
            peer_senders: HashMap::new(),
            recv,
            executed_up_to: 0,
            maxTs: vec![0; replica_nums],
            mytxns: BTreeMap::new(),
            local_node_ids: Vec::new(),
            ycsb,
            // executed: 0,
        };
    }

    pub fn create_ts(&mut self) -> u64 {
        let time = (Local::now().timestamp_nanos() / 1000) as u64;
        let mut ts = time << 12 + self.id << 10;
        if ts < self.maxTs[self.id as usize] {
            let maxts = self.maxTs[self.id as usize] >> 12;
            ts = (maxts + 1) << 12 + (self.id as u64);
        }
        return ts;
    }

    pub async fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::PeerMsg(peer_msg) => match peer_msg.op() {
                TxnOp::ReadOnly => todo!(),
                TxnOp::Prepare => self.handle_prepare(peer_msg).await,
                TxnOp::Accept => todo!(),
                TxnOp::Commit => self.handle_commit(peer_msg).await,
                TxnOp::ReadOnlyRes => todo!(),
                TxnOp::PrepareRes => self.handle_irt_ack(peer_msg).await,
                TxnOp::AcceptRes => todo!(),
                TxnOp::CommitRes => todo!(),
                TxnOp::Abort => todo!(),
            },
            Msg::ClientMsg(client_msg) => self.coordIRT(client_msg).await,
        }
    }

    async fn coordIRT(&mut self, msg: ClientMsg) {
        let mut txn = msg.tmsg;
        let ts = self.create_ts();
        txn.timestamp = ts;
        txn.maxts = ts;
        let txn_in_memory = TxnInMemory::new_with_callback(txn.clone(), msg.callback);
        self.readyq.insert(ts, Some(txn_in_memory.clone()));

        self.mytxns.insert(ts, txn_in_memory);
        // update notified ts
        for notified in self.notifiedTs.iter_mut() {
            *notified = ts;
        }
        // update maxts
        self.maxTs[self.id as usize] = ts;
        txn.from = self.id;
        // println!("prepare {} {:?}", txn.timestamp, get_txnid(txn.txn_id));
        self.broadcast(txn).await;
    }

    async fn handle_prepare(&mut self, msg: DastMsg) {
        let dst_id = msg.from;
        // update maxts
        if msg.timestamp > self.maxTs[msg.from as usize] {
            self.maxTs[msg.from as usize] = msg.timestamp;
        }
        if msg.timestamp > self.maxTs[self.id as usize] {
            self.maxTs[self.id as usize] = msg.timestamp;
        }
        // insert ts into readyq
        for ts in msg.notified_txn_ts.iter() {
            if *ts > self.executed_up_to && !self.readyq.contains_key(ts) {
                self.readyq.insert(*ts, None);
            }
        }
        // println!("insert into rq {}", msg.timestamp);
        self.readyq
            .insert(msg.timestamp, Some(TxnInMemory::new(msg.clone())));
        // println!(
        //     "handle prepare {},{:?}, from{}",
        //     msg.timestamp,
        //     get_txnid(msg.txn_id),
        //     msg.from
        // );
        // println!("self maxts {:?}", self.maxTs);
        // update notified ts
        let mut notified_txn_ts = Vec::new();
        if self.notifiedTs[msg.from as usize] < msg.timestamp {
            let notified_range = self
                .mytxns
                .range(self.notifiedTs[msg.from as usize]..msg.timestamp);
            notified_range.for_each(|(ts, txn)| notified_txn_ts.push(*ts));
        }
        self.notifiedTs[msg.from as usize] = msg.timestamp;
        //reply ack
        let ack = DastMsg {
            txn_id: msg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            notified_txn_ts: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.id,
            timestamp: msg.timestamp,
            txn_type: msg.txn_type,
            maxts: self.maxTs[self.id as usize],
            success: true,
        };
        self.send_to_peer(ack, dst_id).await;
    }

    /**
     * check commit
     */
    async fn handle_irt_ack(&mut self, txn: DastMsg) {
        let from = txn.from as usize;

        // update max ts
        if txn.maxts > self.maxTs[txn.from as usize] {
            self.maxTs[txn.from as usize] = txn.maxts;
        }
        // insert ts into readyq
        for ts in txn.notified_txn_ts.iter() {
            if *ts > self.executed_up_to && !self.readyq.contains_key(ts) {
                self.readyq.insert(*ts, None);
            }
        }
        // println!(
        //     "handle irt ack {},{:?}",
        //     txn.timestamp,
        //     get_txnid(txn.txn_id)
        // );
        match self.readyq.get_mut(&txn.timestamp) {
            Some(txn_im_memory_option) => {
                let mut txn_in_memory = txn_im_memory_option.as_mut().unwrap();
                txn_in_memory.reply_num += 1;
                if txn_in_memory.reply_num == self.majority_size {
                    // commit
                    txn_in_memory.committed = true;
                    let commit = DastMsg {
                        txn_id: txn.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        op: TxnOp::Commit.into(),
                        from: self.id,
                        timestamp: txn.timestamp,
                        txn_type: txn.txn_type,
                        notified_txn_ts: Vec::new(),
                        maxts: self.maxTs[self.id as usize],
                        success: true,
                    };

                    self.broadcast(commit).await;
                    // execute

                    let to_execute = self.check_txn();
                    self.execute_txn(to_execute).await;
                }
            }
            None => return,
        }
    }

    async fn handle_commit(&mut self, msg: DastMsg) {
        // execute
        if self.maxTs[msg.from as usize] < msg.maxts {
            self.maxTs[msg.from as usize] = msg.maxts;
        }

        // println!(
        //     "handle commit {},{:?}, {:?}",
        //     msg.timestamp,
        //     self.maxTs,
        //     get_txnid(msg.txn_id)
        // );
        self.readyq
            .get_mut(&msg.timestamp)
            .unwrap()
            .as_mut()
            .unwrap()
            .committed = true;
        let to_execute = self.check_txn();
        self.execute_txn(to_execute).await;
    }

    async fn coor_crt(&mut self, msg: DastMsg) {}

    async fn handle_crt(&mut self, msg: DastMsg) {}

    fn check_txn(&mut self) -> Vec<TxnInMemory> {
        let mut executed: Vec<TxnInMemory> = Vec::new();
        // println!("check rq size {}", self.readyq.len());
        // if self.readyq.len() > 99 {
        //     panic!("")
        // }
        loop {
            match self.readyq.first_key_value() {
                Some((key, value)) => match value {
                    Some(txn_in_memory) => {
                        if txn_in_memory.committed {
                            let mut safe = true;
                            for max_ts in self.maxTs.iter() {
                                if *max_ts < *key {
                                    safe = false;
                                    break;
                                }
                            }
                            // execute
                            if safe {
                                //
                                // let key = self.readyq.first_key_value().unwrap()
                                let txn = self.readyq.pop_first().unwrap().1.unwrap();

                                executed.push(txn);
                            } else {
                                break;
                            }
                        } else {
                            // println!(
                            //     "execute not commit {:?} {}",
                            //     get_txnid(txn_in_memory.txn.txn_id),
                            //     txn_in_memory.txn.timestamp
                            // );
                            break;
                        }
                    }
                    None => {
                        // println!("NONE {}", *key);

                        break;
                    }
                },
                None => break,
            }
        }
        return executed;
        // for (ts, q) in self.readyq.iter() {
        //     match q {
        //         Some(txn_in_memory) => {
        //             if txn_in_memory.committed {
        //                 // check

        //             } else {
        //                 break;
        //             }
        //         }
        //         None => break,
        //     }
        // }
        // for ts in executed.iter() {
        //     self.readyq.remove(ts);
        // }
    }

    async fn execute_txn(&mut self, txns: Vec<TxnInMemory>) {
        // println!("execute txns {:?}", txns);
        for txn_in_memory in txns.iter() {
            // self.executed += 1;
            self.executed_up_to = txn_in_memory.txn.timestamp;
            let mut reply = txn_in_memory.txn.clone();
            match txn_in_memory.txn.txn_type() {
                rpc::common::TxnType::TatpGetSubscriberData => {}
                rpc::common::TxnType::TatpGetNewDestination => (),
                rpc::common::TxnType::TatpGetAccessData => (),
                rpc::common::TxnType::TatpUpdateSubscriberData => (),
                rpc::common::TxnType::TatpUpdateLocation => (),
                rpc::common::TxnType::TatpInsertCallForwarding => (),
                rpc::common::TxnType::Ycsb => {
                    for write in reply.write_set.iter() {
                        //
                        self.ycsb.insert(write.key, write.value.clone());
                    }
                    reply.write_set.clear();
                    for read in reply.read_set.iter_mut() {
                        //
                        read.value = Some(self.ycsb.get(&read.key).unwrap().clone());
                    }
                }
            }
            match &txn_in_memory.callback {
                Some(callback) => {
                    //
                    // println!("execute {:?}", get_txnid(reply.txn_id));
                    callback.send(reply).await;
                    // unsafe {
                    //     COMMITTED.fetch_add(1, Ordering::Relaxed);
                    // }
                }
                None => continue,
            }
        }
    }

    async fn broadcast(&mut self, msg: DastMsg) {
        for (id, client) in self.peer_senders.iter() {
            client.send(msg.clone()).await;
        }
    }

    async fn broadcast_to_managers(&mut self, msg: DastMsg) {
        for id in self.manager_ids.iter() {
            self.peer_senders.get(id).unwrap().send(msg.clone()).await;
        }
    }

    // async fn broadcast_to_local_nodes(&mut self, msg: DastMsg) {}

    async fn send_to_peer(&mut self, msg: DastMsg, to: u32) {
        let client = self.peer_senders.get(&to).unwrap();
        client.send(msg).await;
    }

    async fn init_rpc(&mut self, sender: UnboundedSender<Msg>) {
        // init peer rpc
        // let mut listen_ip = if self.wide_area {
        //     self.config
        //         .wide_private_server_addr
        //         .get(&self.id)
        //         .unwrap()
        //         .clone()
        // } else {
        //     self.config.server_addrs.get(&self.id).unwrap().clone()
        // };
        let mut listen_ip = self.config.server_addrs.get(&self.id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        info!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, sender.clone());

        tokio::spawn(async move {
            run_rpc_server(server).await;
        });
        // hold the clients to all the server
        for (id, ip) in self.config.server_addrs.iter() {
            if *id != self.id {
                tracing::info!("init client connect to {}", ip);
                // let mut client = PeerCommunicationClient::connect(ip).await?;
                let (send_to_server, server_receiver) = channel::<DastMsg>(10000);
                //init client
                let mut client = RpcClient::new(ip.clone()).await;
                tokio::spawn(async move {
                    client.run_client(server_receiver).await;
                });
                self.peer_senders.insert(id.clone(), send_to_server);
            }
        }
        // start propose server
        // let propose_ip = if self.wide_area {
        //     convert_ip_addr(
        //         self.config.wide_private_propose_addr[&self.id].clone(),
        //         false,
        //     )
        // } else {
        //     convert_ip_addr(self.config.propose_server_addrs[&self.id].clone(), false)
        // };
        let propose_ip = convert_ip_addr(self.config.propose_addrs[&self.id].clone(), false);
        let propose_server = ProposeServer::new(propose_ip, sender);
        tokio::spawn(async move {
            run_propose_server(propose_server).await;
        });
        println!("propose server started");
    }

    pub async fn init_run(&mut self, sender: UnboundedSender<Msg>) {
        self.init_rpc(sender).await;
        let serverid = self.id;
        // tokio::spawn(async move {
        //     let mut throughput = Vec::new();
        //     let mut last = 0;
        //     unsafe {
        //         for _ in 0..15 {
        //             sleep(Duration::from_secs(1)).await;
        //             let now = COMMITTED.load(Ordering::Relaxed);
        //             throughput.push(now - last);
        //             last = now;
        //         }
        //     }
        //     //
        //     let throughput_file_name = serverid.to_string() + "throughput.data";
        //     let mut throughput_file = OpenOptions::new()
        //         .create(true)
        //         .write(true)
        //         .open(throughput_file_name)
        //         .await
        //         .unwrap();
        //     for result in throughput {
        //         throughput_file.write(result.to_string().as_bytes()).await;
        //         throughput_file.write("\n".as_bytes()).await;
        //     }
        //     throughput_file.flush();

        //     println!("finished");
        // });
        loop {
            match self.recv.recv().await {
                Some(msg) => self.handle_msg(msg).await,
                None => continue,
            }
        }
    }
}
