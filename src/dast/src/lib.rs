use rpc::dast::DastMsg;
use tokio::sync::mpsc::Sender;

pub mod executor;
pub mod manager;
pub mod peer;
pub mod peer_communication;

pub fn IsLocalNode() -> bool {
    return true;
}

pub struct Msg {
    pub tmsg: DastMsg,
    pub callback: Sender<DastMsg>,
}
