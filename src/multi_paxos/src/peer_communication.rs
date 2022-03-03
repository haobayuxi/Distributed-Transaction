use rpc::multipaxos_rpc::{
    m_paxos_client::MPaxosClient,
    m_paxos_server::{MPaxos, MPaxosServer},
    MPaxosMsg,
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
    fn m_paxos<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<tonic::Streaming<rpc::multipaxos_rpc::MPaxosMsg>>,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<
                    Output = Result<tonic::Response<rpc::multipaxos_rpc::Reply>, tonic::Status>,
                > + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
