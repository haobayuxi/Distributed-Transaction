use rpc::dast::DastMsg;
use tokio::sync::mpsc::Sender;

pub mod manager;
pub mod peer;
pub mod peer_communication;

pub fn IsLocalNode() -> bool {
    return true;
}

pub enum Msg {
    PeerMsg(DastMsg),
    ClientMsg(ClientMsg),
}

pub struct ClientMsg {
    pub tmsg: DastMsg,
    pub callback: Sender<DastMsg>,
}
