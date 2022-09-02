use std::pin::Pin;

use rpc::yuxi::yuxi_server::Yuxi;
use rpc::yuxi::yuxi_server::YuxiServer;
use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::Status;
use tonic::Streaming;
use tonic::{transport::Channel, Request, Response};

use crate::Msg;

pub struct RpcServer {
    addr_to_listen: String,
    sender: UnboundedSender<Msg>,
    // recv: Receiver<Result<YuxiMsg, Status>>,
}

impl RpcServer {
    pub fn new(
        addr_to_listen: String,
        sender: UnboundedSender<Msg>,
        // recv: Receiver<Result<YuxiMsg, Status>>,
    ) -> Self {
        Self {
            sender,
            addr_to_listen,
            // recv,
        }
    }
}

pub async fn run_rpc_server(rpc_server: RpcServer) {
    let addr = rpc_server.addr_to_listen.parse().unwrap();

    println!("rpc server listening on: {:?}", addr);

    let server = YuxiServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl Yuxi for RpcServer {
    type YuxiTxnStream = ReceiverStream<Result<YuxiMsg, Status>>;
    async fn yuxi_txn(
        &self,
        request: Request<Streaming<YuxiMsg>>,
    ) -> Result<Response<Self::YuxiTxnStream>, Status> {
        let (callback_sender, mut receiver) = channel::<Result<YuxiMsg, Status>>(100);
        let mut in_stream = request.into_inner();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(txn) => {
                        let msg = Msg {
                            tmsg: txn,
                            callback: callback_sender.clone(),
                        };
                        sender.send(msg);
                    }
                    Err(_) => {}
                }
            }
        });

        // self.sender.send(msg);
        // let mut recv = self.recv;
        let out_stream = ReceiverStream::new(receiver);
        // let result = receiver.recv().await.unwrap();
        Ok(Response::new(out_stream))
    }
}
