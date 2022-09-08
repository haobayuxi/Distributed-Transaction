use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use common::{config::Config, convert_ip_addr, ycsb::init_ycsb};
use log::info;
use rpc::yuxi::YuxiMsg;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
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

pub struct Meta {
    pub maxts: TS,
    pub waitlist: WaitList,
    pub smallest_wait_ts: TS,
}

// msg queue
pub static mut IN_MEMORY_MQ: Vec<Vec<Option<Msg>>> = Vec::new();
pub static mut COUNT: usize = 0;

pub struct Peer {
    server_id: u32,

    // dispatcher
    executor_senders: HashMap<u32, Sender<Msg>>,
    msg_queue_index: Vec<usize>,
    executor_num: u32,
    config: Config,
}

impl Peer {
    pub fn new(server_id: u32, config: Config) -> Self {
        // init data

        Self {
            server_id,
            executor_senders: HashMap::new(),
            executor_num: config.executor_num,
            msg_queue_index: vec![0; config.executor_num as usize],
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

    fn init_data(&mut self) -> HashMap<i64, RwLock<(Meta, Vec<VersionData>)>> {
        // init
        unsafe {
            let mut indexs = HashMap::new();
            // self.mem = Arc::new(mem);
            let data = init_ycsb();

            // IN_MEMORY_DATA.reserve(data.len());
            let mut index = 0;
            for (key, value) in data {
                let version_data = VersionData {
                    start_ts: 0,
                    end_ts: MaxTs,
                    data: common::Data::Ycsb(value),
                };
                indexs.insert(
                    key,
                    RwLock::new((
                        Meta {
                            maxts: 0,
                            waitlist: BTreeMap::new(),
                            smallest_wait_ts: MaxTs,
                        },
                        vec![version_data],
                    )),
                );

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

    fn init_executors(
        &mut self,
        config: Config,
        indexs: Arc<HashMap<i64, RwLock<(Meta, Vec<VersionData>)>>>,
    ) {
        // self.executor_num = config.executor_num;
        self.executor_num = config.executor_num;
        for i in 0..self.executor_num {
            // unsafe {
            //     let mut queue: Vec<Option<Msg>> = Vec::new();
            //     for j in 0..1000 {
            //         queue.push(None);
            //     }
            //     IN_MEMORY_MQ.push(queue);
            // }
            // println!("init executor {}", i);
            let (sender, receiver) = channel::<Msg>(1000);
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(i, self.server_id, receiver, indexs.clone());
            tokio::spawn(async move {
                exec.run().await;
            });
        }
    }

    async fn run_dispatcher(&mut self, recv: UnboundedReceiver<Msg>) {
        let mut recv = recv;
        // let mut i = 0;
        loop {
            match recv.recv().await {
                Some(msg) => {
                    // println!("txnid {}", msg.tmsg.txn_id);
                    let executor_id = (msg.tmsg.from as u32) % self.executor_num;
                    // send to executor
                    unsafe {
                        // println!(
                        //     "executor id = {}, from {},txnid{}, index{}, {}",
                        //     executor_id,
                        //     msg.tmsg.from,
                        //     msg.tmsg.txn_id - ((msg.tmsg.from as u64) << 50),
                        //     // msg.tmsg.op()
                        //     self.msg_queue_index[executor_id as usize],
                        //     COUNT
                        // );
                        COUNT += 1;
                        self.executor_senders
                            .get(&executor_id)
                            .unwrap()
                            .send(msg)
                            .await
                            .unwrap();
                        // let index = self.msg_queue_index[executor_id as usize];
                        // IN_MEMORY_MQ[executor_id as usize][index] = Some(msg);
                        // self.msg_queue_index[executor_id as usize] += 1;
                        // if self.msg_queue_index[executor_id as usize] == 1000 {
                        //     self.msg_queue_index[executor_id as usize] = 0;
                        // }
                    }
                }
                None => continue,
            }
        }
    }
}
