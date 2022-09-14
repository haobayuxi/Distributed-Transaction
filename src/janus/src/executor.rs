use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common::{get_client_id, get_txnid};
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};

use crate::{
    peer::DATA,
    peer::{Node, TXNS},
    JanusMeta, Msg,
};

pub struct Executor {
    server_id: u32,
    // memory
    meta_index: Arc<HashMap<i64, usize>>,
    // txns
    // txns: Arc<HashMap<u64, JanusMsg>>,
    //
    recv: UnboundedReceiver<Msg>,
    dep_graph: Sender<u64>,
}

impl Executor {
    pub fn new(
        server_id: u32,
        dep_graph: Sender<u64>,
        recv: UnboundedReceiver<Msg>,
        meta_index: Arc<HashMap<i64, usize>>,
    ) -> Self {
        Self {
            server_id,
            dep_graph,
            recv,
            meta_index,
        }
    }

    pub async fn handle_msg(&mut self, msg: Msg) {
        match msg.txn.op() {
            TxnOp::ReadOnly => self.handle_prepare(msg).await,
            TxnOp::Prepare => self.handle_prepare(msg).await,
            TxnOp::Accept => self.handle_accept(msg).await,
            TxnOp::Commit => self.handle_commit(msg).await,
            TxnOp::ReadOnlyRes => {}
            TxnOp::PrepareRes => {}
            TxnOp::AcceptRes => {}
            TxnOp::CommitRes => {}
            TxnOp::Abort => {}
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    self.handle_msg(msg).await;
                }
                None => continue,
            }
        }
    }

    async fn handle_commit(&mut self, commit: Msg) {
        let txnid = commit.txn.txn_id;

        // self.dep_graph.send(txnid);
        // println!("recv commit {:?}", get_txnid(txnid));
        unsafe {
            let (clientid, index) = get_txnid(txnid);
            let mut waiting = 0;

            let deps = commit.txn.deps;
            let (notify_sender, mut recv) = unbounded_channel::<u64>();
            {
                let mut node = TXNS[clientid as usize][index as usize].write().await;
                node.committed = true;
                if commit.txn.from % 3 == self.server_id {
                    node.callback = Some(commit.callback);
                }

                for dep in deps.iter() {
                    if *dep == 0 {
                        continue;
                    }
                    let (dep_clientid, dep_index) = get_txnid(*dep);

                    let mut next = TXNS[dep_clientid as usize][dep_index as usize]
                        .write()
                        .await;
                    if !next.executed {
                        waiting += 1;
                        next.notify.push(notify_sender.clone());
                    }
                }
                if waiting != 0 {
                    node.waiting_dep = waiting;
                }
            }
            waiting = 0;
            // println!("waiting = {},dep ={:?}", waiting, deps);

            if waiting == 0 {
                // execute
                let meta_index = self.meta_index.clone();
                execute(txnid, meta_index).await;
            } else {
                // update in memory txn
                // println!("spawn to wait {}", waiting);
                self.dep_graph.send(txnid).await;
                let meta_index = self.meta_index.clone();
                tokio::spawn(async move {
                    loop {
                        match recv.recv().await {
                            Some(_) => {
                                waiting -= 1;
                                if waiting == 0 {
                                    execute(txnid, meta_index).await;
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                });
            }
        }
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        // println!("prepare txn {:?}", get_txnid(msg.txn.txn_id));
        let mut result = JanusMsg {
            txn_id: msg.txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            deps: Vec::new(),
            txn_type: None,
        };
        unsafe {
            // get the dep
            for read in msg.txn.read_set.iter() {
                let meta_index = self.meta_index.get(&read.key).unwrap();

                let meta = DATA[*meta_index].0.read().await;
                let dep = meta.last_visited_txnid;
                result.deps.push(dep);
            }

            for write in msg.txn.write_set.iter() {
                let meta_index = self.meta_index.get(&write.key).unwrap();

                let mut meta = DATA[*meta_index].0.write().await;
                let dep = meta.last_visited_txnid;
                meta.last_visited_txnid = msg.txn.txn_id;
                result.deps.push(dep);
            }

            // result.deps.sort();
            let txnid = msg.txn.txn_id;
            let (client_id, index) = get_txnid(txnid);
            TXNS[client_id as usize][index as usize].write().await.txn = Some(msg.txn);
        }
        // reply to coordinator
        msg.callback.send(Ok(result)).await;
    }

    async fn handle_accept(&mut self, msg: Msg) {
        let txnid = msg.txn.txn_id;
        let accept_ok = JanusMsg {
            txn_id: txnid,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::AcceptRes.into(),
            from: self.server_id,
            deps: Vec::new(),
            txn_type: None,
        };
        // self.txns.insert(txnid, msg.txn);
        // reply accept ok to coordinator
        msg.callback.send(Ok(accept_ok)).await;
    }
}

pub async fn execute(txnid: u64, meta_index: Arc<HashMap<i64, usize>>) {
    unsafe {
        let (clientid, index) = get_txnid(txnid);
        let mut node = TXNS[clientid as usize][index as usize].write().await;
        if node.executed {
            return;
        }
        node.executed = true;
        let txn = node.txn.as_ref().unwrap();
        let write_set = txn.write_set.clone();

        if node.callback.is_some() {
            // execute
            let mut result = JanusMsg {
                txn_id: txnid,
                read_set: Vec::new(),
                write_set: Vec::new(),
                op: TxnOp::CommitRes.into(),
                from: 0,
                deps: Vec::new(),
                txn_type: None,
            };

            for read in txn.read_set.iter() {
                let meta_index = meta_index.get(&read.key).unwrap();

                let read_result = ReadStruct {
                    key: read.key.clone(),
                    value: Some(DATA[*meta_index].1.read().await.clone()),
                    timestamp: None,
                };
                result.read_set.push(read_result);
            }
            // println!("execute {:?}", get_txnid(txnid));
            node.callback.take().unwrap().send(Ok(result)).await;
        }
        for write in write_set.iter() {
            let meta_index = meta_index.get(&write.key).unwrap();
            let mut guard = DATA[*meta_index].1.write().await;
            *guard = write.value.clone();
        }
        // notify
        for to_notify in node.notify.iter() {
            to_notify.send(0);
        }
    }
}
