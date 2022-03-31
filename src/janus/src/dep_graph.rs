use std::{
    cmp::{min, Ordering},
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use common::get_client_id;
use rpc::janus::JanusMsg;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::Msg;

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

pub struct DepGraph {
    // dep graph
    graph: HashMap<i64, Node>,
    // wait list
    wait_list: Vec<i64>,
    recv: UnboundedReceiver<Msg>,
    // job senders
    executors: HashMap<i32, UnboundedSender<JanusMsg>>,

    // stack for tarjan
    stack: Vec<i64>,
    index: i32,
    visit: i32,
    // used for tarjan
    waiting_for: i64,
    last_executed: Vec<i64>,
}

impl DepGraph {
    pub fn new(
        executors: HashMap<i32, UnboundedSender<JanusMsg>>,
        recv: UnboundedReceiver<Msg>,
        client_num: usize,
    ) -> Self {
        let mut last_executed = Vec::new();
        last_executed.reserve(client_num);

        Self {
            graph: HashMap::new(),
            executors,
            wait_list: Vec::new(),
            recv,
            stack: Vec::new(),
            index: 0,
            visit: 0,
            last_executed,
            waiting_for: -1,
        }
    }

    async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    let txnid = msg.txn.txn_id;
                    let node = Node::new(msg);
                    self.graph.insert(txnid, node);
                    if self.waiting_for == -1 {
                        // find scc
                        self.find_scc(txnid);
                    } else if self.waiting_for == txnid {
                        // continue find scc
                        let to_find_scc = self.stack[self.visit as usize];
                        self.continue_find_scc(to_find_scc);
                    } else {
                        continue;
                    }
                }
                None => continue,
            }
        }
    }

    fn continue_find_scc(&mut self, txnid: i64) -> bool {
        return true;
    }

    fn find_scc(&mut self, txnid: i64) -> bool {
        self.stack.clear();
        self.visit = 0;
        self.index = 0;
        // insert into stack
        self.stack.push(txnid);
        while self.visit >= 0 {
            let tid = self.stack[self.visit as usize];
            let node = self.graph.get_mut(&tid).unwrap();
            let mut in_stack = false;
            let mut next_dfn = 0;
            if node.low < 0 {
                self.index += 1;
                node.dfn = self.index;
                node.low = self.index;
                let deps = node.msg.txn.deps.clone();
                next_dfn = node.low;
                for dep in deps.iter() {
                    if *dep == 0 {
                        continue;
                    }
                    in_stack = false;
                    match self.graph.get(dep) {
                        Some(next) => {
                            // check if next in the stack
                            if next.dfn < 0 {
                                // not in stack
                                self.stack.push(*dep);
                                self.visit += 1;
                            } else {
                                // if node.low > next.dfn {
                                //     node.low = next.dfn;
                                // }
                                in_stack = true;
                                next_dfn = min(next_dfn, next.dfn)
                            }
                        }
                        None => {
                            // check if next has been executed
                            if *dep < self.last_executed[get_client_id(*dep) as usize] {
                                //executed
                                continue;
                            } else {
                                // has not received the commit msg
                            }
                        }
                    }
                }
                if in_stack {
                    let modify_dfn = self.graph.get_mut(&tid).unwrap();
                    if modify_dfn.low > next_dfn {
                        modify_dfn.low = next_dfn;
                    }
                }
            } else {
                // get scc . pop & exec
                if node.dfn == node.low {
                    let mut to_execute: Vec<JanusMsg> = Vec::new();
                    loop {
                        let tid = self.stack.pop().unwrap();
                        to_execute.push(self.graph.remove(&tid).unwrap().txn);
                        self.visit -= 1;
                        if tid == txnid {
                            break;
                        }
                    }
                    // to execute
                    to_execute.sort_by(|x, y| {
                        if x.txn_id < y.txn_id {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    });
                    // execute & update last_executed
                    for txn in to_execute {
                        if txn.txn_id > self.last_executed[txn.from as usize] {
                            self.last_executed[txn.from as usize] = txn.txn_id;
                        }
                        // send txn to executor
                        todo!()
                    }
                } else {
                    self.visit -= 1;
                }
            }
        }

        return true;
    }
}
