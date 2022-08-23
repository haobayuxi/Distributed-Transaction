use rpc::dast::DastMsg;
use tokio::sync::mpsc::Receiver;

pub struct Executor {
    recv: Receiver<DastMsg>,
}
