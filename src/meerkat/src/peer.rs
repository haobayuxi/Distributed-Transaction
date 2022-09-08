use std::{collections::HashMap, sync::Arc};

use common::{config::Config, convert_ip_addr, ycsb::init_ycsb};
use log::info;
use parking_lot::RwLock;
use rpc::{common::TxnOp, meerkat::MeerkatMsg};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    MeerkatMeta, Msg,
};

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub struct Peer {
    server_id: u32,

    // memory
    mem: Arc<HashMap<i64, RwLock<(MeerkatMeta, String)>>>,
    // dispatcher
    executor_senders: HashMap<u32, UnboundedSender<Msg>>,
    executor_num: u32,
    config: Config,
}

impl Peer {
    pub fn new(server_id: u32, config: Config) -> Self {
        // init data

        let mut mem = HashMap::new();
        // self.mem = Arc::new(mem);
        let data = init_ycsb();
        for (key, value) in data {
            mem.insert(key, RwLock::new((MeerkatMeta::default(), value)));
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

        println!("init data done");
        self.init_executors(self.config.clone());
        self.init_rpc(self.config.clone(), dispatcher_sender).await;
        println!("init rpc done");
        self.run_dispatcher(dispatcher_receiver).await;
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
        self.executor_num = config.executor_num;
        for i in 0..self.executor_num {
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
                    // println!("txnid = {}", msg.tmsg.txn_id);
                    let reply = MeerkatMsg::default();
                    if msg.tmsg.op() != TxnOp::Commit.into() {
                        msg.callback.send(Ok(reply)).await;
                    }
                    // let executor_id = (msg.tmsg.from as u32) % self.executor_num;
                    // // send to executor
                    // self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
