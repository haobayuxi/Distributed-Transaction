use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use common::{config::Config, convert_ip_addr, ycsb::init_ycsb};
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
    MaxTs, Msg, VersionData, WaitList, TS,
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
    executor_senders: HashMap<u32, UnboundedSender<Msg>>,
    executor_num: i32,
    config: Config,
}

impl Peer {
    pub fn new(server_id: i32, config: Config) -> Self {
        // init data

        Self {
            server_id,
            executor_senders: HashMap::new(),
            executor_num: config.executor_num,
            config,
        }
    }

    pub async fn init(&mut self) {
        let (dispatcher_sender, dispatcher_receiver) = unbounded_channel::<Msg>();
        let indexs = self.init_data();
        println!("init data done");
        self.init_executors(self.config.clone(), Arc::new(indexs));
        self.init_rpc(self.config.clone(), dispatcher_sender).await;
        println!("init rpc done");
        self.run_dispatcher(dispatcher_receiver).await;
    }

    fn init_data(&mut self) -> HashMap<i64, usize> {
        // init
        unsafe {
            let mut indexs = HashMap::new();
            // self.mem = Arc::new(mem);
            let data = init_ycsb();

            // IN_MEMORY_DATA.reserve(data.len());
            let mut index = 0;
            for (key, value) in data {
                indexs.insert(key, index);
                // insert to IN_MEMORY_DATA
                let version_data = VersionData {
                    start_ts: 0,
                    end_ts: MaxTs,
                    data: common::Data::Ycsb(value),
                };
                IN_MEMORY_DATA.push((
                    RwLock::new(0),
                    RwLock::new(BTreeMap::new()),
                    RwLock::new(MaxTs),
                    vec![version_data],
                ));
                index += 1;
            }

            indexs
        }
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

    fn init_executors(&mut self, config: Config, indexs: Arc<HashMap<i64, usize>>) {
        self.executor_num = config.executor_num;
        for i in 0..config.executor_num {
            println!("init executor {}", i);
            let (sender, receiver) = unbounded_channel::<Msg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(i, self.server_id, receiver, indexs.clone());
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
                    println!("txnid {}", msg.tmsg.txn_id);
                    let executor_id = (msg.tmsg.txn_id as u32) % self.executor_num;
                    // send to executor
                    // match self.executor_num
                    println!("executor id = {}, self{}", executor_id, self.executor_num);
                    self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
