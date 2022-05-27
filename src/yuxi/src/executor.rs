use tokio::sync::mpsc::UnboundedReceiver;

use crate::Msg;

pub struct Executor {
    id: i32,
    server_id: i32,

    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new() {}
}
