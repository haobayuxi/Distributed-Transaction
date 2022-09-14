use std::{
    collections::{HashMap, HashSet},
    sync::{mpsc::Sender, Arc},
};

use common::{get_client_id, get_txnid};
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{
    apply::Apply,
    dep_graph::{Node, TXNS},
    peer::META,
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
    apply: UnboundedSender<u64>,
}

impl Executor {
    pub fn new(
        server_id: u32,
        meta_index: Arc<HashMap<i64, usize>>,
        dep_graph: Sender<u64>,
        recv: UnboundedReceiver<Msg>,
        apply: UnboundedSender<u64>,
    ) -> Self {
        Self {
            server_id,
            meta_index,
            dep_graph,
            recv,
            apply,
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

    // async fn handle_read_only(&mut self, msg: Msg) {
    //     let txnid = msg.txn.txn_id;
    //     let txn = self.txns.remove(&txnid).unwrap();
    //     // execute
    //     let mut result = JanusMsg {
    //         txn_id: txnid,
    //         read_set: Vec::new(),
    //         write_set: Vec::new(),
    //         op: TxnOp::CommitRes.into(),
    //         from: self.server_id,
    //         deps: Vec::new(),
    //         txn_type: None,
    //     };

    //     for read in txn.read_set {
    //         let read_result = ReadStruct {
    //             key: read.key.clone(),
    //             value: Some(self.mem.get(&read.key).unwrap().read().await.1.clone()),
    //             timestamp: None,
    //         };
    //         result.read_set.push(read_result);
    //     }

    //     // reply to coordinator
    //     msg.callback.send(Ok(result)).await;
    // }

    async fn handle_commit(&mut self, commit: Msg) {
        let txnid = commit.txn.txn_id;

        self.dep_graph.send(txnid);
        // println!("recv commit {:?}", get_txnid(txnid));
        unsafe {
            let (clientid, index) = get_txnid(txnid);
            let node = &mut TXNS[clientid as usize][index as usize];
            node.callback = Some(commit.callback);
            node.txn.deps = commit.txn.deps;
            node.committed = true;

            let mut executed = true;
            for dep in node.txn.deps.iter() {
                if *dep == 0 {
                    continue;
                }
                let (dep_clientid, dep_index) = get_txnid(*dep);
                while TXNS[dep_clientid as usize].len() <= dep_index as usize
                    || !TXNS[dep_clientid as usize][dep_index as usize].committed
                {
                    // not committed
                    executed = false;
                    // break;
                }
                if executed {
                    let next = &mut TXNS[dep_clientid as usize][dep_index as usize];
                    if next.executed {
                        continue;
                    } else {
                        executed = false;
                        break;
                    }
                } else {
                    break;
                }
            }
            if executed {
                node.executed = true;
                // println!("exec execute {}", txnid);
                self.apply.send(txnid);
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
                let index = self.meta_index.get(&read.key).unwrap();
                // let meta = self.meta.get_mut(&read.key).unwrap();
                let meta = META[*index].0.read().await;
                let dep = meta.last_visited_txnid;
                result.deps.push(dep);
            }

            for write in msg.txn.write_set.iter() {
                let index = self.meta_index.get(&write.key).unwrap();
                // let meta = self.meta.get_mut(&read.key).unwrap();
                let mut meta = META[*index].0.write().await;
                let dep = meta.last_visited_txnid;
                meta.last_visited_txnid = msg.txn.txn_id;
                result.deps.push(dep);
            }

            // result.deps.sort();
            let txnid = msg.txn.txn_id;
            let (client_id, index) = get_txnid(txnid);
            TXNS[client_id as usize][index as usize] = Node::new(msg.txn);
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
