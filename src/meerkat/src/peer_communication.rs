use std::time::Duration;

use log::info;
use rpc::meerkat::meerkat_client::MeerkatClient;
use rpc::meerkat::meerkat_server::Meerkat;
use rpc::meerkat::meerkat_server::MeerkatServer;
use rpc::meerkat::MeerkatMsg;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::Status;
use tonic::Streaming;
use tonic::{transport::Channel, Request, Response};

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

    let server = MeerkatServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl Meerkat for RpcServer {
    type TxnMsgStream = ReceiverStream<Result<MeerkatMsg, Status>>;
    async fn txn_msg(
        &self,
        request: Request<Streaming<MeerkatMsg>>,
    ) -> Result<Response<Self::TxnMsgStream>, Status> {
        let (callback_sender, mut receiver) = channel::<Result<MeerkatMsg, Status>>(100);
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
                    Err(_) => {
                        break;
                    }
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
    client: MeerkatClient<Channel>,
    sender: Sender<MeerkatMsg>,
}

impl RpcClient {
    pub async fn new(addr: String, sender: Sender<MeerkatMsg>) -> Self {
        loop {
            match MeerkatClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self { client, sender };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn run_client(&mut self, receiver: Receiver<MeerkatMsg>) {
        let receiver = ReceiverStream::new(receiver);

        let mut response = self.client.txn_msg(receiver).await.unwrap().into_inner();
        while let Some(msg) = response.message().await.unwrap() {
            self.sender.send(msg).await;
        }
    }
}
