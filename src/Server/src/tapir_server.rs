use std::{collections::HashMap, sync::Arc};

use common::{config::Config, convert_ip_addr, ycsb::init_data};
use log::info;
use serde::{Deserialize, Serialize};
use tapir::{
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    Msg, TapirMeta,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub struct Server {
    server_id: i32,

    // memory
    mem: Arc<HashMap<i64, RwLock<(TapirMeta, String)>>>,
    // dispatcher
    executor_senders: HashMap<i32, UnboundedSender<Msg>>,
    executor_num: i32,
    config: Config,
}

impl Server {
    pub fn new(server_id: i32, config: Config) -> Self {
        // init data

        let mut mem = HashMap::new();
        // self.mem = Arc::new(mem);
        let data = init_data(
            config.clone(),
            config.server_ids.get(&server_id).unwrap().clone(),
        );
        for (key, value) in data {
            mem.insert(key, RwLock::new((TapirMeta::default(), value)));
        }

        Self {
            server_id,
            mem: Arc::new(mem),
            executor_senders: HashMap::new(),
            executor_num: 0,
            config,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();
        self.init_data();
        self.init_executors(self.config.clone());
        self.init_rpc(self.config.clone(), dispatcher_sender).await;
        self.run_dispatcher(dispatcher_receiver).await;
    }

    fn init_data(&mut self) {}

    async fn init_rpc(&mut self, config: Config, sender: UnboundedSender<Msg>) {
        // start server for client to connect
        let mut listen_ip = config.server_addrs.get(&self.server_id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        info!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, sender);
        tokio::spawn(async move {
            run_rpc_server(server).await;
        });
    }

    fn init_executors(&mut self, config: Config) {
        self.executor_num = config.executor_num;
        for i in 0..config.executor_num {
            let (sender, receiver) = unbounded_channel::<Msg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new_ycsb(i, self.server_id, self.mem.clone(), receiver);
            tokio::spawn(async move {
                exec.run().await;
            });
        }
    }

    async fn run_dispatcher(&mut self, recv: UnboundedReceiver<Msg>) {
        let mut recv = recv;
        loop {
            match recv.recv().await {
                Some(msg) => {
                    let executor_id = (msg.tmsg.txn_id as i32) % self.executor_num;
                    // send to executor
                    self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigPerServer = serde_yaml::from_reader(f).unwrap();
    let mut server = Server::new(server_config.id, Config::default());
    // server.run_dispatcher(recv)
    server.init().await;
    Ok(())
}
