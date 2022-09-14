use std::{collections::HashMap, sync::Arc, time::Duration};

use common::{get_client_id, get_txnid, CID_LEN};
use rpc::janus::JanusMsg;

use tokio::{
    sync::mpsc::{Receiver, Sender, UnboundedSender},
    time::sleep,
};
use tonic::Status;

use crate::{executor::execute, peer::TXNS};

pub struct DepGraph {
    // dep graph
    // graph: Arc<RwLock<HashMap<i64, Node>>>,
    meta_index: Arc<HashMap<i64, usize>>,
    // wait list
    wait_list: Receiver<u64>,

    // stack for tarjan
    stack: Vec<u64>,
    index: i32,
    visit: i32,
}

impl DepGraph {
    pub fn new(
        wait_list: Receiver<u64>,
        client_num: usize,
        meta_index: Arc<HashMap<i64, usize>>,
    ) -> Self {
        Self {
            wait_list,
            stack: Vec::new(),
            index: 0,
            visit: 0,
            meta_index,
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.wait_list.recv().await {
                Some(txnid) => {
                    self.execute_txn(txnid).await;
                }
                None => {
                    sleep(Duration::from_nanos(10));
                }
            }
        }
    }

    async fn apply(&mut self, txnids: Vec<u64>) {
        unsafe {
            for txnid in txnids {
                execute(txnid, self.meta_index.clone()).await;
            }
        }
    }

    async fn execute_txn(&mut self, txnid: u64) {
        unsafe {
            let (client_id, index) = get_txnid(txnid);

            // println!("try to execute {},{}", client_id, index);
            let node = TXNS[client_id as usize][index as usize].read().await;
            if !node.executed {
                self.find_scc(txnid).await;
            }
        }
    }

    async fn find_scc(&mut self, txnid: u64) -> bool {
        unsafe {
            self.stack.clear();
            self.visit = 0;
            self.index = 0;
            // insert into stack
            self.stack.push(txnid);
            while self.visit >= 0 {
                let tid = self.stack[self.visit as usize];
                let (client_id, index) = get_txnid(tid);
                let mut node = TXNS[client_id as usize][index as usize].write().await;

                // println!(
                //     "find scc {},{}, dfn{}, low{}",
                //     client_id, index, node.dfn, node.low
                // );
                if node.low < 0 {
                    self.index += 1;
                    node.dfn = self.index;
                    node.low = self.index;
                    let deps = node.txn.as_ref().unwrap().deps.clone();
                    for dep in deps {
                        if dep == 0 {
                            continue;
                        }
                        let (dep_clientid, dep_index) = get_txnid(dep);
                        let mut next = TXNS[dep_clientid as usize][dep_index as usize]
                            .write()
                            .await;
                        while !next.committed {
                            // not committed
                            sleep(Duration::from_nanos(100)).await;
                        }

                        if next.executed {
                            continue;
                        }
                        // check if next in the stack
                        if next.dfn < 0 {
                            // not in stack
                            // println!("push into stack {}, {}", dep_clientid, dep_index);
                            next.dfn = 0;
                            self.stack.push(dep);
                            self.visit += 1;
                        } else {
                            if node.low > next.dfn {
                                node.low = next.dfn;
                            }
                        }
                    }
                } else {
                    // get scc . pop & exec
                    if node.dfn == node.low {
                        let mut to_execute: Vec<u64> = Vec::new();
                        loop {
                            let tid = self.stack.pop().unwrap();
                            to_execute.push(tid);
                            self.visit -= 1;
                            if tid == txnid {
                                break;
                            }
                        }
                        // to execute
                        to_execute.sort();
                        // send txn to executor
                        self.apply(to_execute).await;
                    } else {
                        self.visit -= 1;
                    }
                }
            }
        }

        return true;
    }
}
