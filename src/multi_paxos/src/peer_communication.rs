use common::PeerMsg;
use rpc::{
    classic::Reply,
    mpaxos::m_paxos_server::MPaxos,
    mpaxos::{m_paxos_client::MPaxosClient, MPaxosMsg},
};
use std::time::Duration;
use tokio::{
    sync::{
        mpsc::{channel, Receiver, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time::sleep,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{
    codegen::http::request,
    transport::{Channel, Server},
    Request, Response, Status, Streaming,
};
use tracing::info;

pub struct MPaxos_Rpc_Client {
    client: MPaxosClient<Channel>,
}

impl MPaxos_Rpc_Client {
    pub async fn new(addr: String) -> Self {
        loop {
            match MPaxosClient::connect(addr.clone()).await {
                Ok(client) => {
                    return Self { client };
                }
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
    }

    pub async fn run_client(&mut self, receiver: Receiver<MPaxosMsg>) {
        let receiver = ReceiverStream::new(receiver);

        let _response = self.client.replication(receiver).await;
    }
}

pub struct MPaxosServer {
    // sender to dispatcher
    sender: UnboundedSender<PeerMsg>,
}

impl MPaxosServer {
    pub fn new(sender: UnboundedSender<PeerMsg>) -> Self {
        Self { sender }
    }
}

#[tonic::async_trait]
impl MPaxos for MPaxosServer {
    async fn replication(
        &self,
        request: tonic::Request<Streaming<MPaxosMsg>>,
    ) -> Result<tonic::Response<Reply>, tonic::Status> {
        let mut stream = request.into_inner();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(peer_request) = stream.next().await {
                // info!("server receive a msg {:?}", peer_request.clone().unwrap().msg.unwrap());
                match peer_request {
                    Ok(msg) => {
                        sender.send(PeerMsg::MPaxos(msg));
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
