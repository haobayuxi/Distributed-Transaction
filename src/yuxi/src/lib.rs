use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::Sender;

pub mod coordinator;
pub mod executor;
pub mod peer;
pub mod peer_communication;

pub struct Msg {
    pub tmsg: YuxiMsg,
    pub callback: Sender<YuxiMsg>,
}
