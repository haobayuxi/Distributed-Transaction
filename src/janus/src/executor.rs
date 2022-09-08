use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common::{get_client_id, get_txnid};
use rpc::{
    common::{ReadStruct, TxnOp},
    janus::JanusMsg,
};
use tokio::sync::{
    mpsc::{Sender, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{
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
    dep_graph: UnboundedSender<u64>,
}

impl Executor {
    pub fn new(
        server_id: u32,
        meta_index: Arc<HashMap<i64, usize>>,
        dep_graph: UnboundedSender<u64>,
        recv: UnboundedReceiver<Msg>,
    ) -> Self {
        Self {
            server_id,
            meta_index,
            dep_graph,
            recv,
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
        // println!("recv commit {:?}", get_txnid(txnid));
        unsafe {
            let (clientid, index) = get_txnid(txnid);
            let node = &mut TXNS[clientid as usize][index as usize];
            node.callback = Some(commit.callback);
            node.txn.deps = commit.txn.deps;
            node.committed = true;
            self.dep_graph.send(txnid);

            // let mut result = JanusMsg {
            //     txn_id: txnid,
            //     read_set: Vec::new(),
            //     write_set: Vec::new(),
            //     op: TxnOp::CommitRes.into(),
            //     from: self.server_id,
            //     deps: Vec::new(),
            //     txn_type: None,
            // };
            // // reply to coordinator
            // if commit.txn.from % 3 == self.server_id {
            //     node.callback.take().unwrap().send(Ok(result)).await;
            // }
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
            let mut result_dep = HashSet::new();
            // get the dep
            for read in msg.txn.read_set.iter() {
                let index = self.meta_index.get(&read.key).unwrap();
                // let meta = self.meta.get_mut(&read.key).unwrap();
                let meta = META[*index].read().await;
                let dep = meta.last_visited_txnid;
                // meta.last_visited_txnid = msg.txn.txn_id;
                // result.deps.push(dep);
                result_dep.insert(dep);
            }

            for write in msg.txn.write_set.iter() {
                let index = self.meta_index.get(&write.key).unwrap();
                // let meta = self.meta.get_mut(&read.key).unwrap();
                let mut meta = META[*index].write().await;
                let dep = meta.last_visited_txnid;
                meta.last_visited_txnid = msg.txn.txn_id;
                // result.deps.push(dep);
                result_dep.insert(dep);
            }
            for iter in result_dep.into_iter() {
                result.deps.push(iter);
            }

            result.deps.sort();
            let txnid = msg.txn.txn_id;
            let node = Node::new(msg.txn);
            let client_id = get_client_id(txnid);
            TXNS[client_id as usize].push(node);
        }
        // self.txns.insert(msg.txn.txn_id, msg.txn);
        // reply to coordinator
        msg.callback.send(Ok(result)).await;
        // println!("send back prepareok {:?}", result);
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
