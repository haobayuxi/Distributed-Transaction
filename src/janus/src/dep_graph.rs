use std::{collections::HashMap, hash::Hash};

use rpc::janus::JanusMsg;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

struct Node {
    executed: bool,
    txn: Option<JanusMsg>,
    children: Vec<i64>,
}
pub struct DepGraph {
    // dep graph
    graph: HashMap<i64, Node>,
    // job senders
    executors: HashMap<i32, UnboundedSender<JanusMsg>>,
    // recv
    recv: UnboundedReceiver<JanusMsg>,
    //
}

impl DepGraph {
    pub fn new(
        executors: HashMap<i32, UnboundedSender<JanusMsg>>,
        recv: UnboundedReceiver<JanusMsg>,
    ) -> Self {
        Self {
            graph: HashMap::new(),
            executors,
            recv,
        }
    }

    async fn run(&mut self) {
        loop {
            let commit = self.recv.recv().await.unwrap();
            // add txn into graph
            match self.graph.get_mut(&commit.txn_id) {
                Some(node) => {
                    node.txn = Some(commit.clone());
                }
                None => {
                    let commit_node = Node {
                        executed: false,
                        txn: Some(commit.clone()),
                        children: Vec::new(),
                    };
                    self.graph.insert(commit.txn_id, commit_node);
                }
            }

            for dep in commit.deps.iter() {
                // check if the dep has been committed
                match self.graph.get(dep) {
                    Some(node) => todo!(),
                    None => {
                        // add txn into graph
                    }
                }
            }
        }
    }
}
