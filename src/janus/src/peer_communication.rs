use std::collections::HashMap;

use rpc::janus::{janus_server::Janus, JanusMsg, TxnOp};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tonic::{Request, Response, Status};

use crate::{shard_txn_to_executors, Msg};

pub struct RpcServer {
    senders: HashMap<i32, UnboundedSender<Msg>>,
    send_to_dep_graph: UnboundedSender<Msg>,
}

impl RpcServer {
    pub fn new(
        senders: HashMap<i32, UnboundedSender<Msg>>,
        send_to_dep_graph: UnboundedSender<Msg>,
    ) -> Self {
        Self {
            senders,
            send_to_dep_graph,
        }
    }
}

#[tonic::async_trait]
impl Janus for RpcServer {
    async fn janus_txn(&self, request: Request<JanusMsg>) -> Result<Response<JanusMsg>, Status> {
        let (sender, mut receiver) = unbounded_channel::<JanusMsg>();
        // let msg = Msg {
        //     txn: request.into_inner(),
        //     callback: sender,
        // };
        // dispatch txn to executors
        let txn = request.into_inner();

        let mut result = JanusMsg {
            txn_id: txn.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            executor_ids: Vec::new(),
            op: txn.op,
            from: 0,
            deps: Vec::new(),
        };
        match txn.op() {
            TxnOp::Commit => {
                // commit msg only contains executor ids, the transaction are store in each executor
                let mut pieces = txn.executor_ids.len();
                let msg = Msg {
                    txn,
                    callback: sender,
                };
                self.send_to_dep_graph.send(msg);
                // join the result
                while pieces > 0 {
                    let mut msg = receiver.recv().await.unwrap();
                    result.op = msg.op;
                    result.read_set.append(msg.read_set.as_mut());
                    result.deps.append(msg.deps.as_mut());
                    result.from = msg.from;
                    pieces -= 1;
                }
            }
            _ => {
                let txns = shard_txn_to_executors(txn);
                let mut pieces = txns.len();
                for (id, txn_per_executor) in txns {
                    let executor_sender = self.senders.get(&id).unwrap();
                    let msg = Msg {
                        txn: txn_per_executor,
                        callback: sender.clone(),
                    };
                    result.executor_ids.push(id);
                    executor_sender.send(msg);
                }

                // join the result
                while pieces > 0 {
                    let mut msg = receiver.recv().await.unwrap();
                    result.op = msg.op;
                    result.read_set.append(msg.read_set.as_mut());
                    result.deps.append(msg.deps.as_mut());
                    result.from = msg.from;
                    pieces -= 1;
                }
            }
        }

        Ok(Response::new(result))
    }
}
