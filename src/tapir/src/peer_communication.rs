use rpc::tapir::tapir_server::Tapir;
use rpc::tapir::TapirMsg;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;
use tonic::{transport::Channel, Request, Response};

use crate::Msg;

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
