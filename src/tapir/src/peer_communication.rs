use std::time::Duration;

use rpc::tapir::tapir_server::Tapir;
use rpc::tapir::TapirMsg;
use rpc::{classic::Reply, tapir::tapir_client::TapirClient};
use tokio::sync::mpsc::channel;
use tokio::{
    sync::mpsc::{Receiver, UnboundedSender},
    time::sleep,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::{transport::Channel, Request, Response, Streaming};

use crate::Msg;

pub struct RpcClient {
    client: TapirClient<Channel>,
}

impl RpcClient {
    pub async fn new(addr: String) -> Self {
        loop {
            match TapirClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self { client };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

pub struct RpcServer {
    sender: UnboundedSender<Msg>,
}

impl RpcServer {
    pub fn new(sender: UnboundedSender<Msg>) -> Self {
        Self { sender }
    }
}

#[tonic::async_trait]
impl Tapir for RpcServer {
    async fn txn_msg(&self, request: Request<TapirMsg>) -> Result<Response<TapirMsg>, Status> {
        let (sender, mut receiver) = channel::<TapirMsg>(1);
        let msg = Msg {
            tmsg: request.into_inner(),
            callback: sender,
        };
        self.sender.send(msg);

        let result = receiver.recv().await.unwrap();
        Ok(Response::new(result))
    }
}
