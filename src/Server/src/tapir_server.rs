use std::{collections::HashMap, hash::Hash, sync::Arc};

use rpc::tapir::TapirMsg;
use tapir::{executor::Executor, Msg};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    RwLock,
};

pub struct Server {
    server_id: i32,

    // memory
    mem: Arc<HashMap<String, Arc<RwLock<(i64, String)>>>>,
    // dispatcher
    executor_senders: HashMap<i32, UnboundedSender<Msg>>,
    executor_num: i32,
}

impl Server {
    pub fn new(server_id: i32) {
        //
    }

    pub async fn init(&mut self) {
        self.init_data();
        self.init_executors();
        self.init_rpc().await;
    }

    fn init_data(&mut self) {
        let mut mem = HashMap::new();
        mem.insert(
            "test".to_string(),
            Arc::new(RwLock::new((0, "testvalue".to_string()))),
        );
        self.mem = Arc::new(mem);
    }

    async fn init_rpc(&mut self) {
        // start server for client to connect
    }

    fn init_executors(&mut self) {
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<Msg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(i, self.server_id, self.mem.clone(), receiver);
            tokio::spawn(async move {
                exec.run().await;
            });
        }
    }

    fn run_dispatcher(&mut self) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
