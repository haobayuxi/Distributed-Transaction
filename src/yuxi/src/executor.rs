use tokio::sync::mpsc::UnboundedReceiver;

use crate::Msg;

pub struct Executor {
    id: i32,
    server_id: i32,

    recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub fn new() {}

    pub fn run(&mut self) {
        
    }

    async fn handle_prepare(&mut self, msg : Msg) {
        
    }

    async fn handle_accept(&mut self, msg: Msg) {

    }
}
