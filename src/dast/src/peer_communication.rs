use std::time::Duration;

use log::info;
use rpc::dast::client_service_client::ClientServiceClient;
use rpc::dast::client_service_server::ClientService;
use rpc::dast::client_service_server::ClientServiceServer;
use rpc::dast::dast_server::Dast;
use rpc::dast::dast_server::DastServer;
use rpc::dast::DastMsg;
use rpc::dast::Reply;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::Status;
use tonic::Streaming;
use tonic::{transport::Channel, Request, Response};

use crate::ClientMsg;
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

    let server = DastServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl Dast for RpcServer {
    async fn txn_msg(
        &self,
        request: Request<Streaming<DastMsg>>,
    ) -> Result<Response<Reply>, Status> {
        let mut stream = request.into_inner();
        while let Some(peer_request) = stream.next().await {
            // info!("server receive a msg {:?}", peer_request.clone().unwrap().msg.unwrap());
            match peer_request {
                Ok(msg) => {
                    self.sender.send(Msg::PeerMsg(msg));
                }
                Err(_) => {
                    //todo handle network err
                }
            };
        }
        let reply = Reply {};
        Ok(Response::new(reply))
    }
}

pub struct ProposeClient {
    addr_to_connect: String,
    client: ClientServiceClient<Channel>,
}

impl ProposeClient {
    pub async fn new(addr: String) -> Self {
        loop {
            match ClientServiceClient::connect(addr.clone()).await {
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
}

pub struct ProposeServer {
    addr_to_listen: String,
    sender: UnboundedSender<Msg>,
    // receiver: Arc<Mutex<UnboundedReceiver<ClientMsgReply>>>,
}

impl ProposeServer {
    pub fn new(
        addr_to_listen: String,
        sender: UnboundedSender<Msg>,
        // receiver: UnboundedReceiver<ClientMsgReply>,
    ) -> Self {
        Self {
            addr_to_listen,
            sender,
            // receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}

pub async fn run_propose_server(propose_server: ProposeServer) {
    let addr = propose_server.addr_to_listen.parse().unwrap();

    tracing::info!("propose server listening on: {:?}", addr);

    let server = ClientServiceServer::new(propose_server);

    match Server::builder().add_service(server).serve(addr).await {
        Ok(_) => tracing::info!("propose rpc server start done"),
        Err(e) => panic!("propose rpc server start fail {}", e),
    }
}

#[tonic::async_trait]
impl ClientService for ProposeServer {
    async fn propose(
        &self,
        request: tonic::Request<DastMsg>,
    ) -> Result<tonic::Response<DastMsg>, tonic::Status> {
        let msg = request.into_inner();
        let (callback, mut callback_recv) = channel::<DastMsg>(1);
        self.sender.send(Msg::ClientMsg(ClientMsg {
            tmsg: msg,
            callback,
        }));

        // reply to client
        let result = callback_recv.recv().await.unwrap();
        Ok(Response::new(result))
    }
}
