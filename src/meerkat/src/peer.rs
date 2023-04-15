use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use common::{config::Config, convert_ip_addr, ycsb::init_ycsb};
use log::info;
use parking_lot::RwLock;
use rpc::{common::TxnOp, meerkat::MeerkatMsg};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::sleep,
    time::Duration,
};

use crate::{
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    MeerkatMeta, Msg,
};

// pub static mut COMMITTED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub struct Peer {
    server_id: u32,

    // memory
    mem: Arc<HashMap<u64, RwLock<(MeerkatMeta, String)>>>,
    // dispatcher
    // executor_senders: Arc<HashMap<u32, UnboundedSender<Msg>>>,
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
            // executor_senders: HashMap::new(),
            executor_num: 0,
            config,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();

        println!("init data done");
        let executor_senders = self.init_executors(self.config.clone());
        self.init_rpc(self.config.clone(), executor_senders).await;
        println!("init rpc done");
    }

    async fn init_rpc(
        &mut self,
        config: Config,
        // sender: UnboundedSender<Msg>,
        executor_senders: Arc<HashMap<u32, UnboundedSender<Msg>>>,
    ) {
        // start server for client to connect
        let mut listen_ip = config.server_addrs.get(&self.server_id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, executor_senders);
        // tokio::spawn(async move {
        run_rpc_server(server).await;
        // });
    }

    fn init_executors(&mut self, config: Config) -> Arc<HashMap<u32, UnboundedSender<Msg>>> {
        self.executor_num = config.executor_num;
        let mut executor_senders = HashMap::new();
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<Msg>();
            executor_senders.insert(i, sender);
            let mut exec = Executor::new_ycsb(i, self.server_id, self.mem.clone(), receiver);
            tokio::spawn(async move {
                exec.run().await;
            });
        }
        Arc::new(executor_senders)
    }

    // async fn run_dispatcher(&mut self, recv: UnboundedReceiver<Msg>) {
    //     let serverid = self.server_id;
    //     // tokio::spawn(async move {

    //     // });
    //     let mut recv = recv;
    //     // loop {
    //     //     match recv.recv().await {
    //     //         Some(msg) => {
    //     //             // println!("txnid = {}", msg.tmsg.txn_id);

    //     //             let executor_id = (msg.tmsg.from as u32) % self.executor_num;
    //     //             // send to executor
    //     //             self.executor_senders.get(&executor_id).unwrap().send(msg);
    //     //         }
    //     //         None => continue,
    //     //     }
    //     // }
    // }
}
