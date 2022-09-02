use std::{collections::HashMap, sync::Arc};

use common::{config::Config, convert_ip_addr, ycsb::init_data};
use log::info;
use rpc::yuxi::YuxiMsg;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

use crate::{
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    Msg, VersionData, WaitList, TS,
};

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

// (maxts, wait list, waiting ts, versiondata)
pub static mut IN_MEMORY_DATA: Vec<(RwLock<TS>, RwLock<WaitList>, RwLock<TS>, Vec<VersionData>)> =
    Vec::new();

pub struct Peer {
    server_id: i32,

    // dispatcher
    executor_senders: HashMap<i32, UnboundedSender<Msg>>,
    executor_num: i32,
    config: Config,
}

impl Peer {
    pub fn new(server_id: i32, config: Config) -> Self {
        // init data

        // self.mem = Arc::new(mem);
        let data = init_data(
            config.clone(),
            config.server_ids.get(&server_id).unwrap().clone(),
        );

        Self {
            server_id,
            executor_senders: HashMap::new(),
            executor_num: 0,
            config,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();
        self.init_data();
        println!("init data done");
        self.init_executors(self.config.clone());
        self.init_rpc(self.config.clone(), dispatcher_sender).await;
        println!("init rpc done");
        self.run_dispatcher(dispatcher_receiver).await;
    }

    fn init_data(&mut self) {
        // init

        // init
    }

    async fn init_rpc(&mut self, config: Config, sender: UnboundedSender<Msg>) {
        // start server for client to connect
        let mut listen_ip = config.server_addrs.get(&self.server_id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, sender);
        tokio::spawn(async move {
            run_rpc_server(server).await;
        });
    }

    fn init_executors(&mut self, config: Config) {
        // self.executor_num = config.executor_num;
        // for i in 0..config.executor_num {
        //     let (sender, receiver) = unbounded_channel::<Msg>();
        //     self.executor_senders.insert(i, sender);
        //     let mut exec = Executor::new_ycsb(i, self.server_id, self.mem.clone(), receiver);
        //     tokio::spawn(async move {
        //         exec.run().await;
        //     });
        // }
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
