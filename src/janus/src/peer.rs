use std::{collections::HashMap, sync::Arc};

use common::{config::Config, convert_ip_addr, ycsb::init_ycsb};
use log::info;
use serde::{Deserialize, Serialize};

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{
    dep_graph::DepGraph,
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    JanusMeta, Msg,
};

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub struct Peer {
    server_id: u32,

    // memory
    // mem: Arc<HashMap<i64, RwLock<(JanusMeta, String)>>>,
    // dispatcher
    // executor_senders: HashMap<i32, UnboundedSender<Msg>>,
    executor: Executor,
    config: Config,
}

impl Peer {
    pub fn new(server_id: u32, config: Config) -> Self {
        // init data

        let mut mem = HashMap::new();
        // self.mem = Arc::new(mem);
        let data = init_ycsb();
        for (key, value) in data {
            mem.insert(key, RwLock::new((JanusMeta::default(), value)));
        }

        let executor = Executor::new(server_id, mem);

        Self {
            server_id,
            // mem: Arc::new(mem),
            // executor_senders: HashMap::new(),
            config,
            executor,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();
        // self.init_data();
        println!("init data done");
        let (dep_sender, dep_receiver) = unbounded_channel::<Msg>();
        self.init_dep(
            dispatcher_sender.clone(),
            dep_receiver,
            self.config.client_num as usize,
        )
        .await;
        // self.init_executors(self.config.clone());
        self.init_rpc(self.config.clone(), dispatcher_sender, dep_sender)
            .await;
        println!("init rpc done");
        self.run_dispatcher(dispatcher_receiver).await;
    }

    async fn init_dep(
        &mut self,
        sender: UnboundedSender<Msg>,
        recv: UnboundedReceiver<Msg>,
        client_num: usize,
    ) {
        let mut dep_graph = DepGraph::new(sender, recv, client_num);
        tokio::spawn(async move {
            dep_graph.run().await;
        });
    }

    async fn init_rpc(
        &mut self,
        config: Config,
        sender: UnboundedSender<Msg>,
        send_to_dep_graph: UnboundedSender<Msg>,
    ) {
        // start server for client to connect
        let mut listen_ip = config.server_addrs.get(&self.server_id).unwrap().clone();
        listen_ip = convert_ip_addr(listen_ip, false);
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, sender, send_to_dep_graph);
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
                    self.executor.handle_msg(msg).await;
                    // let executor_id = (msg.txn.txn_id as i32) % self.executor_num;
                    // // send to executor
                    // self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
