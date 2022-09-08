use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    peer::{Meta, COUNT, IN_MEMORY_MQ},
    ExecuteContext, MaxTs, Msg, VersionData, TS,
};
use common::{
    get_txnid,
    tatp::{AccessInfo, CallForwarding, Subscriber},
    Data,
};
use parking_lot::RwLock;
use rpc::{
    common::{ReadStruct, TxnOp, WriteStruct},
    yuxi::YuxiMsg,
};
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender};
use tokio::time::sleep;
// use tokio::time::Duration;

pub struct Executor {
    executor_id: u32,
    server_id: u32,

    replica_num: u32,
    recv: Receiver<Msg>,
    // sender: Sender<Msg>,
    // cache the txn in memory
    msg_queue_index: usize,
    // Vec<TS> is used to index the write in waitlist
    txns: HashMap<u64, (YuxiMsg, Vec<(WriteStruct, TS)>)>,
    // ycsb
    index: Arc<HashMap<i64, RwLock<(Meta, Vec<VersionData>)>>>,
    // tatp
    subscriber: Arc<HashMap<u64, usize>>,
    access_info: Arc<HashMap<u64, usize>>,
    special_facility: Arc<HashMap<u64, usize>>,
    call_forwarding: Arc<HashMap<u64, usize>>,
}

impl Executor {
    pub fn new(
        executor_id: u32,
        server_id: u32,
        recv: Receiver<Msg>,
        // sender: Sender<Msg>,
        index: Arc<HashMap<i64, RwLock<(Meta, Vec<VersionData>)>>>,
    ) -> Self {
        Self {
            executor_id,
            server_id,
            replica_num: 3,
            recv,
            msg_queue_index: 0,
            // sender,
            txns: HashMap::new(),
            index,
            subscriber: Arc::new(HashMap::new()),
            access_info: Arc::new(HashMap::new()),
            special_facility: Arc::new(HashMap::new()),
            call_forwarding: Arc::new(HashMap::new()),
        }
    }

    pub async fn run(&mut self) {
        // let mut i = 0;
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    self.handle_msg(msg).await;
                    // i += 1;
                    // unsafe {
                    //     println!("handle msg {} {} ", i, COUNT);
                    // }
                }
                None => {
                    sleep(Duration::from_millis(2)).await;
                }
            }
            // }
            // unsafe {
            //     match IN_MEMORY_MQ[self.executor_id as usize][self.msg_queue_index].take() {
            //         Some(msg) => {
            //             self.handle_msg(msg).await;
            //             println!("handle msg {}, {}", self.msg_queue_index, i);
            //             i += 1;
            //             self.msg_queue_index += 1;
            //             if self.msg_queue_index == 1000 {
            //                 self.msg_queue_index = 0;
            //             }
            //         }
            //         None => {
            //             println!(
            //                 "executor msg queue empty {},{},{}",
            //                 self.msg_queue_index, i, COUNT
            //             );
            //             if i == COUNT {
            //                 sleep(Duration::from_millis(500)).await;
            //             }
            //             sleep(Duration::from_millis(20)).await;
            //         }
            //     }
            // }
        }
    }

    async fn handle_msg(&mut self, msg: Msg) {
        match msg.tmsg.op() {
            TxnOp::Abort => {}
            TxnOp::ReadOnly => self.handle_read_only(msg).await,
            TxnOp::Commit => self.handle_commit(msg).await,
            TxnOp::Prepare => self.handle_prepare(msg).await,
            TxnOp::PrepareRes => {}
            TxnOp::ReadOnlyRes => {}
            TxnOp::Accept => self.handle_accept(msg).await,
            TxnOp::AcceptRes => {}
            TxnOp::CommitRes => {}
        }
    }

    async fn handle_read_only(&mut self, msg: Msg) {
        // just wait for the earlier txn to be executed

        let mut txn = msg.tmsg.clone();
        let final_ts = txn.timestamp;
        let mut waiting_for_read_result = 0;
        let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
        let mut read_set = txn.read_set.clone();
        txn.read_set.clear();
        for read in read_set.iter_mut() {
            let key = read.key;
            let mut tuple = self.index.get(&key).unwrap().write();

            let meta = &mut tuple.0;
            if meta.maxts < final_ts {
                meta.maxts = final_ts
            }

            if final_ts > meta.smallest_wait_ts {
                waiting_for_read_result += 1;
                // insert a read task
                let execution_context = ExecuteContext {
                    committed: true,
                    read: true,
                    value: None,
                    call_back: Some(sender.clone()),
                    txnid: txn.txn_id,
                };
                // let mut wait_list = tuple.1.write().await;
                meta.waitlist.insert(final_ts, execution_context);
                continue;
            }

            let version_data = &tuple.1;
            let mut index = version_data.len() - 1;
            while final_ts < version_data[index].start_ts {
                index -= 1;
            }
            let data = version_data[index].data.to_string();
            txn.read_set.push(ReadStruct {
                key,
                value: Some(data),
                timestamp: None,
            });
        }
        if waiting_for_read_result == 0 {
            // reply to coordinator
            msg.callback.send(Ok(txn)).await;
        } else {
            // spawn a new task for this
            tokio::spawn(async move {
                while waiting_for_read_result > 0 {
                    if let Some((key, value)) = receiver.recv().await {
                        txn.read_set.push(ReadStruct {
                            key,
                            value: Some(value),
                            timestamp: None,
                        });
                    }
                    waiting_for_read_result -= 1;
                }
                msg.callback.send(Ok(txn)).await;
            });
        }
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        //
        // println!(
        //     " prepare {},{},{}",
        //     self.executor_id,
        //     msg.tmsg.from,
        //     msg.tmsg.txn_id - ((msg.tmsg.from as u64) << 50)
        // );
        let mut prepare_response = YuxiMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::PrepareRes.into(),
            from: self.server_id,
            timestamp: msg.tmsg.timestamp,
            txn_type: msg.tmsg.txn_type,
        };

        let ts = msg.tmsg.timestamp;
        let mut write_ts_in_waitlist = Vec::new();
        for write in msg.tmsg.write_set.iter() {
            let key = write.key;
            let mut tuple = self.index.get(&key).unwrap().write();
            {
                let meta = &mut tuple.0;
                if ts > meta.maxts {
                    meta.maxts = ts;
                } else {
                    meta.maxts += 1;
                    if prepare_response.timestamp < meta.maxts {
                        prepare_response.timestamp = meta.maxts;
                    }
                }
                // insert into wait list
                let execute_context = ExecuteContext {
                    committed: false,
                    read: false,
                    value: Some(write.value.clone()),
                    call_back: None,
                    txnid: msg.tmsg.txn_id,
                };
                if meta.smallest_wait_ts > meta.maxts {
                    meta.smallest_wait_ts = meta.maxts;
                }
                let wait_ts = meta.maxts;
                // println!("insert waitlist {},{}", key, wait_ts);
                meta.waitlist.insert(wait_ts, execute_context);
                write_ts_in_waitlist.push((write.clone(), wait_ts));
            }
        }

        // println!(
        //     "wait list tid{},{:?}",
        //     msg.tmsg.txn_id, write_ts_in_waitlist
        // );
        self.txns
            .insert(msg.tmsg.txn_id, (msg.tmsg.clone(), write_ts_in_waitlist));
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            // find and update the ts
            {
                let mut tuple = self.index.get(&key).unwrap().write();
                let meta = &mut tuple.0;
                if ts > meta.maxts {
                    meta.maxts = ts;
                } else {
                    if prepare_response.timestamp < meta.maxts {
                        prepare_response.timestamp = meta.maxts;
                    }
                }
            }
        }

        // send back result
        msg.callback.send(Ok(prepare_response)).await;
    }

    async fn handle_accept(&mut self, msg: Msg) {
        // reply accept ok
        let result = YuxiMsg {
            txn_id: msg.tmsg.txn_id,
            read_set: Vec::new(),
            write_set: Vec::new(),
            op: TxnOp::AcceptRes.into(),
            from: self.server_id,
            timestamp: 0,
            txn_type: msg.tmsg.txn_type,
        };
        msg.callback.send(Ok(result)).await;
    }

    // async fn handle_abort(&mut self, msg: Msg) {
    //     // abort the transaction
    //     let tid = msg.tmsg.txn_id;
    //     let (txn, write_ts) = self.txns.remove(&tid).unwrap();

    //     // for read in msg.tmsg.read_set.iter() {
    //     //     let key = read.key;
    //     //     // find and erase the ts
    //     //     let mut guard = self.mem.get(&key).unwrap().write().await;
    //     // }
    //     unsafe {
    //         for i in 0..write_ts.len() {}
    //         for write in txn.write_set.iter() {
    //             let key = write.key;
    //             let index = self.index.get(&key).unwrap();
    //             let mut guard = IN_MEMORY_DATA[*index].1.write().await;
    //             // erase the ts from the wait list
    //         }
    //     }
    // }

    async fn handle_commit(&mut self, msg: Msg) {
        // commit final version and execute
        let tid = msg.tmsg.txn_id;
        let final_ts = msg.tmsg.timestamp;
        // get write_ts in waitlist to erase
        let (mut txn, write_ts_in_waitlist) = self.txns.remove(&tid).unwrap();
        let isreply = if self.server_id == (tid as u32) % 3 {
            true
        } else {
            false
        };
        // println!(
        //     "commit txid {},{},{}",
        //     self.executor_id,
        //     msg.tmsg.from,
        //     msg.tmsg.txn_id - ((msg.tmsg.from as u64) << 50),
        // );
        // println!("check write {},{:?}", tid, write_ts_in_waitlist);
        for (write, write_ts) in write_ts_in_waitlist.iter() {
            let key = write.key;

            let mut tuple = self.index.get(&key).unwrap().write();
            // let meta = &mut tuple.0;
            if tuple.0.maxts < final_ts {
                tuple.0.maxts = final_ts
            }

            // modify the wait list
            // println!(
            //     "tuple remove {:?} ,{},{}, {:?}",
            //     get_txnid(tid),
            //     key,
            //     *write_ts,
            //     tuple.0.waitlist
            // );

            if let Some(mut execution_context) = tuple.0.waitlist.remove(write_ts) {
                execution_context.committed = true;
                tuple.0.waitlist.insert(final_ts, execution_context);
            }
            // check pending txns execute the context if the write is committed
            loop {
                match tuple.0.waitlist.pop_first() {
                    Some((ts, mut context)) => {
                        if context.committed {
                            if context.read {
                                // execute the read
                                // get data
                                let version_data = &tuple.1;
                                let mut index = version_data.len() - 1;
                                while final_ts < version_data[index].start_ts {
                                    index -= 1;
                                }
                                let data = version_data[index].data.to_string();
                                let callback = context.call_back.take().unwrap();
                                callback.send((ts as i64, data));
                            } else {
                                // execute the write
                                let datas = &mut tuple.1;
                                match datas.last_mut() {
                                    Some(last_data) => {
                                        last_data.end_ts = ts;
                                    }
                                    None => {}
                                }
                                let mut version_data = VersionData {
                                    start_ts: ts,
                                    end_ts: MaxTs,
                                    data: Data::default(),
                                };
                                match txn.txn_type() {
                                    rpc::common::TxnType::TatpGetSubscriberData => {}
                                    rpc::common::TxnType::TatpGetNewDestination => {}
                                    rpc::common::TxnType::TatpGetAccessData => {}
                                    rpc::common::TxnType::TatpUpdateSubscriberData => {}
                                    rpc::common::TxnType::TatpUpdateLocation => {}
                                    rpc::common::TxnType::TatpInsertCallForwarding => {}
                                    rpc::common::TxnType::Ycsb => {
                                        version_data.data = Data::Ycsb(write.value.clone());
                                    }
                                }
                                datas.push(version_data);
                            }
                        } else {
                            tuple.0.waitlist.insert(ts, context);
                            if tuple.0.smallest_wait_ts < ts {
                                tuple.0.smallest_wait_ts = ts;
                            }
                            break;
                        }
                    }
                    None => {
                        tuple.0.smallest_wait_ts = MaxTs;
                        break;
                    }
                }
            }
        }

        // execute read

        let mut waiting_for_read_result = 0;
        let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
        let mut read_set = txn.read_set.clone();
        txn.read_set.clear();
        for read in read_set.iter_mut() {
            let key = read.key;
            {
                let mut tuple = self.index.get(&key).unwrap().write();
                let meta = &mut tuple.0;
                if meta.maxts < final_ts {
                    meta.maxts = final_ts
                }
                if isreply {
                    // get data
                    if final_ts > meta.smallest_wait_ts {
                        waiting_for_read_result += 1;
                        // insert a read task
                        let execution_context = ExecuteContext {
                            committed: true,
                            read: true,
                            value: None,
                            call_back: Some(sender.clone()),
                            txnid: tid,
                        };
                        // let mut wait_list = tuple.1.write().await;
                        meta.waitlist.insert(final_ts, execution_context);
                        continue;
                    }
                    let version_data = &tuple.1;
                    let mut index = version_data.len() - 1;
                    while final_ts < version_data[index].start_ts {
                        index -= 1;
                    }
                    let data = version_data[index].data.to_string();
                    txn.read_set.push(ReadStruct {
                        key,
                        value: Some(data),
                        timestamp: None,
                    });
                }
            }
        }
        // println!("is reply {}, need wait?", isreply);
        if isreply {
            // do we need to wait
            if waiting_for_read_result == 0 {
                // reply to coordinator
                msg.callback.send(Ok(txn)).await;
            } else {
                // spawn a new task for this
                tokio::spawn(async move {
                    while waiting_for_read_result > 0 {
                        match receiver.recv().await {
                            Some((key, value)) => {
                                txn.read_set.push(ReadStruct {
                                    key,
                                    value: Some(value),
                                    timestamp: None,
                                });
                            }
                            None => {}
                        }

                        waiting_for_read_result -= 1;
                    }
                    msg.callback.send(Ok(txn)).await;
                });
            }
        }
    }
}
