use rpc::janus::{janus_server::Janus, JanusMsg};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tonic::{Request, Response, Status};

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
impl Janus for RpcServer {
    async fn janus_txn(&self, request: Request<JanusMsg>) -> Result<Response<JanusMsg>, Status> {
        let (sender, mut receiver) = unbounded_channel::<JanusMsg>();
        let msg = Msg {
            txn: request.into_inner(),
            callback: sender,
        };
        self.sender.send(msg);

        let result = receiver.recv().await.unwrap();
        Ok(Response::new(result))
    }
}
