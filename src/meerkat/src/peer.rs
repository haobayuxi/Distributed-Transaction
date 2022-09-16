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

pub static mut COMMITTED: AtomicU64 = AtomicU64::new(0);

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
                    // println!("txnid = {}", msg.tmsg.txn_id);

                    let executor_id = (msg.tmsg.from as u32) % self.executor_num;
                    // send to executor
                    self.executor_senders.get(&executor_id).unwrap().send(msg);
                }
                None => continue,
            }
        }
    }
}
