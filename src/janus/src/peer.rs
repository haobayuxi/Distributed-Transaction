use std::{collections::HashMap, sync::Arc};

use common::{config::Config, convert_ip_addr, get_txnid, ycsb::init_ycsb};
use log::info;
use serde::{Deserialize, Serialize};

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{
    apply::Apply,
    dep_graph::DepGraph,
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    JanusMeta, Msg,
};

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub static mut META: Vec<RwLock<JanusMeta>> = Vec::new();

pub struct Peer {
    server_id: u32,

    // memory
    // mem: Arc<HashMap<i64, RwLock<(JanusMeta, String)>>>,
    // dispatcher
    executor_senders: HashMap<u32, UnboundedSender<Msg>>,
    // executor: Executor,
    config: Config,
    executor_num: u32,
}

impl Peer {
    pub fn new(server_id: u32, config: Config) -> Self {
        // init data

        // self.mem = Arc::new(mem);
        let data = init_ycsb();
        let mut meta = HashMap::new();
        unsafe {
            let mut index: usize = 0;
            for (id, value) in data.iter() {
                META.push(RwLock::new(JanusMeta::default()));
                meta.insert(*id, index);
                index += 1;
            }
        }

        let (dep_sender, dep_receiver) = unbounded_channel::<u64>();
        let (apply_sender, apply_receiver) = unbounded_channel::<u64>();

        let mut dep_graph = DepGraph::new(apply_sender, dep_receiver, config.client_num as usize);
        tokio::spawn(async move {
            dep_graph.run().await;
        });
        // self.init_dep(apply_sender, dep_receiver, self.config.client_num as usize)
        //     .await;
        let mut apply = Apply::new(apply_receiver, data, server_id);
        tokio::spawn(async move {
            apply.run().await;
        });
        let arc_meta = Arc::new(meta);
        let mut exec_senders = HashMap::new();
        for i in 0..config.executor_num {
            let (exec_sender, exec_recv) = unbounded_channel::<Msg>();
            let mut executor =
                Executor::new(server_id, arc_meta.clone(), dep_sender.clone(), exec_recv);
            exec_senders.insert(i, exec_sender);
            tokio::spawn(async move {
                executor.run().await;
            });
        }

        Self {
            server_id,
            // executor_senders: HashMap::new(),
            executor_num: config.executor_num,
            config,
            executor_senders: exec_senders,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();
        // self.init_data();
        println!("init data done");

        // self.init_executors(self.config.clone());
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

    // fn init_executors(&mut self, config: Config) {
    //     self.executor_num = config.executor_num;
    //     for i in 0..config.executor_num {
    //         let (sender, receiver) = unbounded_channel::<Msg>();
    //         self.executor_senders.insert(i, sender);
    //         let mut exec = Executor::new_ycsb(i, self.server_id, self.mem.clone(), receiver);
    //         tokio::spawn(async move {
    //             exec.run().await;
    //         });
    //     }
    // }

    async fn run_dispatcher(&mut self, recv: UnboundedReceiver<Msg>) {
        let mut recv = recv;
        loop {
            match recv.recv().await {
                Some(msg) => {
                    // println!("handle msg {:?}", get_txnid(msg.txn.txn_id));
                    // self.executor.handle_msg(msg).await;
                    let executor_id = (msg.txn.txn_id as u32) % self.executor_num;
                    // // send to executor
                    self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
