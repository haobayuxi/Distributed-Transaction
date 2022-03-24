use std::collections::HashMap;

use common::convert_ip_addr;
use rpc::classic::{communication_server::CommunicationServer, Msgs, Txn};
use tokio::sync::mpsc::UnboundedSender;
use tonic::transport::Server;

use crate::peer_communication::RpcServer;

pub struct twoPLServer {
    id: i32,
    peer_ips: HashMap<i32, String>,
    // sender to dispatcher
    sender: UnboundedSender<Msgs>,
}

impl twoPLServer {
    // pub fn new()->Self{}

    pub async fn init_rpc(&mut self) {
        // init peer rpc
        let mut listen_ip = self.peer_ips.get(&self.id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        tracing::info!("server listen ip {}", listen_ip);
        let rpc_server = RpcServer::new(self.sender.clone());
        let addr = listen_ip.parse().unwrap();

        tracing::info!("PeerServer listening on: {:?}", addr);

        let server = CommunicationServer::new(rpc_server);
        // init mpaxos rpc
        // let mpaxos_server =
        match Server::builder().add_service(server).serve(addr).await {
            Ok(_) => tracing::info!("rpc server start done"),
            Err(e) => panic!("rpc server start fail {}", e),
        }
        // init rpc client to connect to other peers
    }
}
