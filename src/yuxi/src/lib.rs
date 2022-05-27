use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::Sender;

pub mod executor;

pub struct Msg {
    pub tmsg: YuxiMsg,
    pub callback: Sender<YuxiMsg>,
}
