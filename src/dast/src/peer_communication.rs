use log::info;
use rpc::dast::dast_server::Dast;
use rpc::dast::dast_server::DastServer;
use rpc::dast::DastMsg;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::UnboundedSender;
use tonic::transport::Server;
use tonic::Status;
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

    let server = DastServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl Dast for RpcServer {
    async fn txn_msg(&self, request: Request<DastMsg>) -> Result<Response<DastMsg>, Status> {
        let (sender, mut receiver) = channel::<DastMsg>(1);
        let msg = Msg {
            tmsg: request.into_inner(),
            callback: sender,
        };
        self.sender.send(msg);

        let result = receiver.recv().await.unwrap();
        Ok(Response::new(result))
    }
}
