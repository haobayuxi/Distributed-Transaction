use std::{collections::HashMap, time::Duration};

use rpc::{
    common::TxnOp,
    janus::{
        janus_client::JanusClient,
        janus_server::{Janus, JanusServer},
        JanusMsg,
    },
};
use tokio::{
    sync::mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedSender},
    time::sleep,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status, Streaming,
};

use crate::Msg;

pub struct RpcServer {
    addr_to_listen: String,
    sender: UnboundedSender<Msg>,
}

impl RpcServer {
    pub fn new(addr_to_listen: String, sender: UnboundedSender<Msg>) -> Self {
        Self {
            sender,
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
    type JanusTxnStream = ReceiverStream<Result<JanusMsg, Status>>;
    async fn janus_txn(
        &self,
        request: Request<Streaming<JanusMsg>>,
    ) -> Result<Response<Self::JanusTxnStream>, Status> {
        let (callback_sender, mut receiver) = channel::<Result<JanusMsg, Status>>(100);
        let mut in_stream = request.into_inner();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(txn) => {
                        let msg = Msg {
                            txn,
                            callback: callback_sender.clone(),
                        };
                        // match txn.op() {
                        //     TxnOp::Commit => {
                        //         // commit msg only contains executor ids
                        //         let msg = Msg {
                        //             txn,
                        //             callback: callback_sender.clone(),
                        //         };
                        //         send_to_dep_graph.send(msg);
                        //         // let mut msg = receiver.recv().await.unwrap();
                        //         // result.read_set = msg.read_set;
                        //     }
                        //     _ => {
                        //         let msg = Msg {
                        //             txn,
                        //             callback: callback_sender.clone(),
                        //         };
                        //         sender.send(msg);
                        //         // result = receiver.recv().await.unwrap();
                        //     }
                        // }
                        sender.send(msg);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        let out_stream = ReceiverStream::new(receiver);
        // let result = receiver.recv().await.unwrap();
        Ok(Response::new(out_stream))
    }
}

pub struct RpcClient {
    client: JanusClient<Channel>,
    sender: Sender<JanusMsg>,
}

impl RpcClient {
    pub async fn new(addr: String, sender: Sender<JanusMsg>) -> Self {
        loop {
            match JanusClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self { client, sender };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn run_client(&mut self, receiver: Receiver<JanusMsg>) {
        let receiver = ReceiverStream::new(receiver);

        let mut response = self.client.janus_txn(receiver).await.unwrap().into_inner();
        while let Some(msg) = response.message().await.unwrap() {
            self.sender.send(msg).await;
        }
    }
}
