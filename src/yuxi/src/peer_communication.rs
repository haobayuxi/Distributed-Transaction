use std::pin::Pin;
use std::time::Duration;

use rpc::yuxi::yuxi_client::YuxiClient;
use rpc::yuxi::yuxi_server::Yuxi;
use rpc::yuxi::yuxi_server::YuxiServer;
use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
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

pub struct RpcClient {
    client: YuxiClient<Channel>,
    sender: Sender<YuxiMsg>,
}

impl RpcClient {
    pub async fn new(addr: String, sender: Sender<YuxiMsg>) -> Self {
        loop {
            match YuxiClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self { client, sender };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn run_client(&mut self, receiver: Receiver<YuxiMsg>) {
        let receiver = ReceiverStream::new(receiver);

        let mut response = self.client.yuxi_txn(receiver).await.unwrap().into_inner();
        while let Some(msg) = response.message().await.unwrap() {
            self.sender.send(msg).await;
        }
    }
}
