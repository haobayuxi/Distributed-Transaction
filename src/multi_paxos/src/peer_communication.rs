use rpc::multipaxos_rpc::{
    m_paxos_client::MPaxosClient,
    m_paxos_server::{MPaxos, MPaxosServer},
    MPaxosMsg, Reply,
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

        let _response = self.client.m_paxos(receiver).await;
    }
}

pub struct MPaxos_Rpc_Server {
    addr_to_listen: String,
    sender: UnboundedSender<MPaxosMsg>,
}

impl MPaxos_Rpc_Server {
    pub fn new(addr_to_listen: String, sender: UnboundedSender<MPaxosMsg>) -> Self {
        Self {
            addr_to_listen,
            sender,
        }
    }
}

pub async fn run_server(rpc_server: MPaxos_Rpc_Server) {
    let addr = rpc_server.addr_to_listen.parse().unwrap();

    tracing::info!("PeerServer listening on: {:?}", addr);

    let server = MPaxosServer::new(rpc_server);

    match Server::builder().add_service(server).serve(addr).await {
        Ok(_) => tracing::info!("rpc server start done"),
        Err(e) => panic!("rpc server start fail {}", e),
    }
}

#[tonic::async_trait]
impl MPaxos for MPaxos_Rpc_Server {
    async fn m_paxos(
        &self,
        request: Request<Streaming<MPaxosMsg>>,
    ) -> Result<Response<Reply>, Status> {
        let mut stream = request.into_inner();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(peer_request) = stream.next().await {
                match peer_request {
                    Ok(msg) => {
                        sender.send(msg);
                    }
                    Err(_) => {}
                }
            }
        });
        let reply = Reply {};
        Ok(Response::new(reply))
    }
}
