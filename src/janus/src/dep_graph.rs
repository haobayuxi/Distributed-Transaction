use std::{
    cmp::{min, Ordering},
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use common::{get_client_id, get_txnid, CID_LEN};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Notify, RwLock,
    },
    time::sleep,
};

use crate::Msg;

static mut TXNS: Vec<Vec<Node>> = Vec::new();

struct Node {
    executed: bool,
    msg: Msg,
    // tarjan
    dfn: i32,
    low: i32,
}

impl Node {
    fn new(msg: Msg) -> Self {
        Self {
            executed: false,
            msg,
            dfn: -1,
            low: -1,
        }
    }
}

// commit msg only contains executor ids , so just send txnid&callback to each executor to execute

pub struct DepGraph {
    // dep graph
    // graph: Arc<RwLock<HashMap<i64, Node>>>,
    // wait list
    wait_list: UnboundedReceiver<u64>,
    // recv: UnboundedReceiver<Msg>,
    // job senders
    executor: UnboundedSender<Msg>,

    // stack for tarjan
    stack: Vec<u64>,
    index: i32,
    visit: i32,
}

impl DepGraph {
    pub fn new(
        executor: UnboundedSender<Msg>,
        recv: UnboundedReceiver<Msg>,
        client_num: usize,
    ) -> Self {
        // init TXNS
        unsafe {
            for i in 0..client_num {
                TXNS.push(vec![]);
            }
            // TXNS.reserve(client_num);
        }

        let (waitlist_sender, waitlist_receiver) = unbounded_channel::<u64>();
        // let graph_clone = graph.clone();
        tokio::spawn(async move {
            unsafe {
                let mut recv = recv;
                loop {
                    match recv.recv().await {
                        Some(commit) => {
                            let txnid = commit.txn.txn_id;
                            let node = Node::new(commit);
                            let client_id = get_client_id(txnid);
                            TXNS[client_id as usize].push(node);
                            waitlist_sender.send(txnid);
                        }
                        None => continue,
                    }
                }
            }
        });
        Self {
            executor,
            wait_list: waitlist_receiver,
            // recv,
            stack: Vec::new(),
            index: 0,
            visit: 0,
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.wait_list.recv().await {
                Some(txnid) => {
                    self.execute_txn(txnid).await;
                }
                None => continue,
            }
        }
    }

    fn apply(&mut self, txn: Msg) {
        let txnid = txn.txn.txn_id;
        let (client_id, index) = get_txnid(txnid);
        println!("send execute {:?}", get_txnid(txnid));
        unsafe {
            TXNS[client_id as usize][index as usize].executed = true;
        }
        self.executor.send(txn);
    }

    async fn execute_txn(&mut self, txnid: u64) {
        unsafe {
            let (client_id, index) = get_txnid(txnid);

            // println!("try to execute {},{}", client_id, index);
            let node = &TXNS[client_id as usize][index as usize];
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
                // let client_id = get_client_id(tid);
                // let index = tid >> CID_LEN;
                let (client_id, index) = get_txnid(tid);
                let mut node = TXNS[client_id as usize].get_mut(index as usize).unwrap();

                println!(
                    "find scc {},{}, dfn{}, low{}",
                    client_id, index, node.dfn, node.low
                );
                if node.low < 0 {
                    self.index += 1;
                    node.dfn = self.index;
                    node.low = self.index;
                    for dep in node.msg.txn.deps.clone() {
                        if dep == 0 {
                            continue;
                        }
                        // let dep_index = dep >> CID_LEN;
                        // let dep_clientid = get_client_id(dep);
                        let (dep_clientid, dep_index) = get_txnid(dep);
                        while dep_index >= TXNS[dep_clientid as usize].len() as u64 {
                            // not committed
                            sleep(Duration::from_nanos(10)).await;
                        }
                        if TXNS[dep_clientid as usize][dep_index as usize].executed {
                            continue;
                        }
                        let next = &TXNS[dep_clientid as usize][dep_index as usize];
                        // check if next in the stack
                        if next.dfn < 0 {
                            // not in stack
                            println!("push into stack {}, {}", dep_clientid, dep_index);
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
                        let mut to_execute: Vec<Msg> = Vec::new();
                        loop {
                            let tid = self.stack.pop().unwrap();
                            // to_execute.push(self.graph.remove(&tid).unwrap().txn);
                            let (client_id, index) = get_txnid(tid);
                            to_execute.push(TXNS[client_id as usize][index as usize].msg.clone());
                            self.visit -= 1;
                            if tid == txnid {
                                break;
                            }
                        }
                        // to execute
                        to_execute.sort_by(|x, y| {
                            if x.txn.txn_id < y.txn.txn_id {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        });
                        // execute & update last_executed
                        for msg in to_execute {
                            // send txn to executor
                            self.apply(msg);
                        }
                    } else {
                        self.visit -= 1;
                    }
                }
            }
        }

        return true;
    }
}
