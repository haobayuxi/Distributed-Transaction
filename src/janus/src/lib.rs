use rpc::janus::JanusMsg;
use tokio::sync::mpsc::UnboundedSender;

mod coordinator;
mod dep_graph;
mod executor;
mod peer_communication;

pub struct Msg {
    txn: JanusMsg,
    callback: UnboundedSender<JanusMsg>,
}
