use std::collections::HashMap;

use common::EXECUTOR_NUM;
use rpc::janus::JanusMsg;
use tokio::sync::mpsc::UnboundedSender;

pub mod coordinator;
pub mod dep_graph;
pub mod executor;
pub mod peer;
pub mod peer_communication;

#[derive(Clone)]
pub struct Msg {
    pub txn: JanusMsg,
    pub callback: UnboundedSender<JanusMsg>,
}

#[derive(Default, Clone)]
pub struct JanusMeta {
    pub last_visited_txnid: i64,
}

pub fn shard_txn_to_executors(txn: JanusMsg) -> HashMap<i32, JanusMsg> {
    let mut result: HashMap<i32, JanusMsg> = HashMap::new();
    for read in txn.read_set {
        let executor_id = (read.key as i32) % EXECUTOR_NUM;
        if result.contains_key(&executor_id) {
            let msg = result.get_mut(&executor_id).unwrap();
            msg.read_set.push(read);
        } else {
            let msg = JanusMsg {
                txn_id: txn.txn_id,
                read_set: vec![read],
                write_set: Vec::new(),
                executor_ids: Vec::new(),
                op: txn.op,
                from: txn.from,
                deps: txn.deps.clone(),
                txn_type: None,
            };
            result.insert(executor_id, msg);
        }
    }

    for write in txn.write_set {
        let executor_id = (write.key as i32) % EXECUTOR_NUM;
        if result.contains_key(&executor_id) {
            let msg = result.get_mut(&executor_id).unwrap();
            msg.write_set.push(write);
        } else {
            let msg = JanusMsg {
                txn_id: txn.txn_id,
                read_set: Vec::new(),
                write_set: vec![write],
                executor_ids: Vec::new(),
                op: txn.op,
                from: txn.from,
                deps: txn.deps.clone(),
                txn_type: None,
            };
            result.insert(executor_id, msg);
        }
    }

    result
}
