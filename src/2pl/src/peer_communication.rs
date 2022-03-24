use std::time::Duration;

use rpc::classic::{
    communication_client::CommunicationClient,
    communication_server::{Communication, CommunicationServer},
    Msgs, Reply,
};
use tokio::{
    sync::mpsc::{Receiver, UnboundedSender},
    time::sleep,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status, Streaming,
};

pub struct RpcClient {
    addr_to_connect: String,
    client: CommunicationClient<Channel>,
}

impl RpcClient {
    pub async fn new(addr: String) -> Self {
        loop {
            match CommunicationClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self {
                        addr_to_connect: addr,
                        client,
                    };
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn run_client(&mut self, receiver: Receiver<Msgs>) {
        // todo the batch here
        let receiver = ReceiverStream::new(receiver);

        let _response = self.client.txn_msg(receiver).await;
    }
}

pub struct RpcServer {
    sender: UnboundedSender<Msgs>,
}

impl RpcServer {
    pub fn new(sender: UnboundedSender<Msgs>) -> Self {
        Self { sender }
    }
}

#[tonic::async_trait]
impl Communication for RpcServer {
    async fn txn_msg(&self, request: Request<Streaming<Msgs>>) -> Result<Response<Reply>, Status> {
        let mut stream = request.into_inner();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(peer_request) = stream.next().await {
                // info!("server receive a msg {:?}", peer_request.clone().unwrap().msg.unwrap());
                match peer_request {
                    Ok(msg) => {
                        // sender.send(PeerMsg::Msg(msg));
                    }
                    Err(_) => {
                        //todo handle network err
                    }
                };
            }
        });
        let reply = Reply {};
        Ok(Response::new(reply))
    }
}
