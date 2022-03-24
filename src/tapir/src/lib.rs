use rpc::tapir::TapirMsg;
use tokio::sync::mpsc::Sender;

pub mod coordinator;
pub mod executor;
pub mod peer_communication;
pub mod server;

pub struct Msg {
    tmsg: TapirMsg,
    callback: Sender<TapirMsg>,
}
