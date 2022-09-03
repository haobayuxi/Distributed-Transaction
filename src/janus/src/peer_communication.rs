use std::collections::HashMap;

use rpc::{
    common::TxnOp,
    janus::{
        janus_server::{Janus, JanusServer},
        JanusMsg,
    },
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tonic::{transport::Server, Request, Response, Status};

use crate::Msg;

pub struct RpcServer {
    addr_to_listen: String,
    sender: UnboundedSender<Msg>,
    send_to_dep_graph: UnboundedSender<Msg>,
}

impl RpcServer {
    pub fn new(
        addr_to_listen: String,
        sender: UnboundedSender<Msg>,
        send_to_dep_graph: UnboundedSender<Msg>,
    ) -> Self {
        Self {
            sender,
            send_to_dep_graph,
            addr_to_listen,
        }
    }
}

pub async fn run_rpc_server(rpc_server: RpcServer) {
    let addr = rpc_server.addr_to_listen.parse().unwrap();

    println!("rpc server listening on: {:?}", addr);

    let server = JanusServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
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
            op: txn.op,
            from: 0,
            deps: Vec::new(),
            txn_type: None,
        };
        match txn.op() {
            TxnOp::Commit => {
                // commit msg only contains executor ids
                let msg = Msg {
                    txn,
                    callback: sender,
                };
                self.send_to_dep_graph.send(msg);
                let mut msg = receiver.recv().await.unwrap();
                result.read_set = msg.read_set;
            }
            _ => {
                let msg = Msg {
                    txn,
                    callback: sender,
                };
                self.sender.send(msg);
                result = receiver.recv().await.unwrap();
                // let txns = shard_txn_to_executors(txn);
                // let mut pieces = txns.len();
                // for (id, txn_per_executor) in txns {
                //     let executor_sender = self.senders.get(&id).unwrap();
                //     let msg = Msg {
                //         txn: txn_per_executor,
                //         callback: sender.clone(),
                //     };
                //     result.executor_ids.push(id);
                //     executor_sender.send(msg);
                // }

                // // join the result
                // while pieces > 0 {
                //     let mut msg = receiver.recv().await.unwrap();
                //     result.op = msg.op;
                //     result.read_set.append(msg.read_set.as_mut());
                //     result.deps.append(msg.deps.as_mut());
                //     result.from = msg.from;
                //     pieces -= 1;
                // }
            }
        }

        Ok(Response::new(result))
    }
}
