use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use common::{config::Config, convert_ip_addr, get_txnid, ycsb::init_ycsb};
use log::info;
// use parking_lot::RwLock;
use rpc::janus::JanusMsg;
use serde::{Deserialize, Serialize};

use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{
        mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    task::spawn_blocking,
    time::sleep,
    time::Duration,
};
use tonic::Status;

use crate::{
    dep_graph::DepGraph,
    executor::Executor,
    peer_communication::{run_rpc_server, RpcServer},
    JanusMeta, Msg,
};

pub static mut TXNS: Vec<Vec<Node>> = Vec::new();
pub static mut DATA: Vec<(RwLock<JanusMeta>, RwLock<String>)> = Vec::new();
pub static mut COMMITTED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Node {
    pub executed: bool,
    pub committed: bool,
    pub waiting_dep: i32,
    pub notify: RwLock<Vec<UnboundedSender<u64>>>,
    // msg: Msg,
    pub txn: Option<JanusMsg>,
    pub callback: Option<Sender<Result<JanusMsg, Status>>>,
    // tarjan
    pub dfn: i32,
    pub low: i32,
}

impl Node {
    pub fn new(txn: JanusMsg) -> Self {
        Self {
            executed: false,
            committed: false,
            txn: Some(txn),
            callback: None,
            notify: RwLock::new(Vec::new()),
            dfn: -1,
            low: -1,
            waiting_dep: 0,
        }
    }
    pub fn default() -> Self {
        Self {
            executed: false,
            committed: false,
            txn: None,
            callback: None,
            notify: RwLock::new(Vec::new()),
            dfn: -1,
            low: -1,
            waiting_dep: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPerServer {
    id: i32,
}

pub struct Peer {
    server_id: u32,

    // memory
    // ycsb: HashMap<i64, (RwLock<JanusMeta>, RwLock<String>)>,
    // dispatcher
    executor_senders: HashMap<u32, UnboundedSender<Msg>>,
    // executor: Executor,
    config: Config,
    executor_num: u32,
}

impl Peer {
    pub fn new(server_id: u32, config: Config) -> Self {
        // init data
        unsafe {
            for _ in 0..config.client_num {
                let mut in_memory_node = Vec::new();
                for _ in 0..10000 {
                    in_memory_node.push(Node::default());
                }
                TXNS.push(Vec::new());
            }
        }
        // self.mem = Arc::new(mem);
        let data = init_ycsb();
        let mut meta_indexs = HashMap::new();
        unsafe {
            let mut index: usize = 0;
            for (id, value) in data.iter() {
                DATA.push((
                    RwLock::new(JanusMeta::default()),
                    RwLock::new(value.clone()),
                ));
                meta_indexs.insert(*id, index);
                index += 1;
            }
        }

        let meta_index = Arc::new(meta_indexs);

        let (dep_sender, dep_receiver) = channel::<u64>(10000);
        // let (apply_sender, apply_receiver) = unbounded_channel::<u64>();

        let mut dep_graph =
            DepGraph::new(dep_receiver, config.client_num as usize, meta_index.clone());
        tokio::spawn(async move {
            dep_graph.run().await;
        });
        // let mut apply = Apply::new(apply_receiver, data, server_id);
        // tokio::spawn(async move {
        //     apply.run().await;
        // });
        let mut exec_senders = HashMap::new();
        for i in 0..config.executor_num {
            let (exec_sender, exec_recv) = unbounded_channel::<Msg>();
            let mut executor =
                Executor::new(server_id, dep_sender.clone(), exec_recv, meta_index.clone());
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
        let serverid = self.server_id;
        tokio::spawn(async move {
            let mut throughput = Vec::new();
            let mut last = 0;
            unsafe {
                for _ in 0..15 {
                    sleep(Duration::from_secs(1)).await;
                    let now = COMMITTED.load(Ordering::Relaxed);
                    throughput.push((now - last) / 3);
                    last = now;
                }
            }
            //
            let throughput_file_name = serverid.to_string() + "throughput.data";
            let mut throughput_file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(throughput_file_name)
                .await
                .unwrap();
            for result in throughput {
                throughput_file.write(result.to_string().as_bytes()).await;
                throughput_file.write("\n".as_bytes()).await;
            }
            throughput_file.flush();

            println!("finished");
        });

        let mut recv = recv;
        loop {
            match recv.recv().await {
                Some(msg) => {
                    // println!("handle msg {:?}", get_txnid(msg.txn.txn_id));
                    // self.executor.handle_msg(msg).await;
                    let executor_id = (msg.txn.from as u32) % self.executor_num;
                    // // send to executor
                    self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
