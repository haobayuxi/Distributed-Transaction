use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    peer::{Meta, DATA},
    ExecuteContext, MaxTs, Msg, VersionData, TS,
};
use common::{
    get_txnid,
    tatp::{AccessInfo, CallForwarding, Subscriber},
    Data,
};
// use parking_lot::RwLock;
use rpc::{
    common::{ReadStruct, TxnOp, TxnType, WriteStruct},
    yuxi::YuxiMsg,
};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, Receiver, Sender},
    RwLock,
};
use tokio::time::sleep;
// use tokio::time::Duration;

fn get_data(ts: u64, index: usize) -> String {
    unsafe {
        let version_data = &DATA[index];
        let mut index = version_data.len() - 1;
        while ts <= version_data[index].start_ts {
            index -= 1;
            if index == 0 {
                break;
            }
        }
        version_data[index].data.to_string()
    }
}
pub struct Executor {
    executor_id: u32,
    server_id: u32,
    recv: Receiver<Msg>,
    // cache the txn in memory
    // Vec<TS> is used to index the write in waitlist
    txns: HashMap<u64, (YuxiMsg, Vec<(WriteStruct, TS)>)>,
    // ycsb
    index: Arc<HashMap<i64, (RwLock<Meta>, usize)>>,
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
        index: Arc<HashMap<i64, (RwLock<Meta>, usize)>>,
    ) -> Self {
        Self {
            executor_id,
            server_id,
            recv,
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
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    self.handle_msg(msg).await;
                }
                None => {
                    sleep(Duration::from_millis(2)).await;
                }
            }
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
        // let mut msg = msg;
        let mut txn = msg.tmsg.clone();
        let final_ts = txn.timestamp;
        let mut waiting_for_read_result = 0;
        let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
        let read_set = txn.read_set.clone();
        txn.read_set.clear();
        for read in read_set.iter() {
            let key = read.key;

            let (meta_rwlock, data_index) = self.index.get(&key).unwrap();
            {
                let mut meta = meta_rwlock.write().await;
                if meta.maxts < final_ts {
                    meta.maxts = final_ts;
                }

                if final_ts > meta.smallest_wait_ts {
                    // println!("final ts {}, wait for {}", final_ts, meta.smallest_wait_ts);
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
            }
            txn.read_set.push(ReadStruct {
                key,
                value: Some(get_data(final_ts, *data_index)),
                timestamp: None,
            });
        }

        if waiting_for_read_result == 0 {
            // reply to coordinator
            msg.callback.send(Ok(txn.clone())).await;
            // println!("send back result {:?}", get_txnid(txnid));
        } else {
            // unsafe {
            //     let from = txn.from;
            //     {
            //         let mut wait_txn = WAITING_TXN[from as usize].write().await;
            //         wait_txn.waiting = waiting_for_read_result;
            //         if wait_txn.waiting == wait_txn.read_set.len() {
            //             println!("send back result {:?}", get_txnid(txnid));
            //             msg.callback.send(Ok(txn.clone())).await;
            //         } else {
            //             println!(
            //                 "{:?}wait {},{}",
            //                 get_txnid(txn.txn_id),
            //                 wait_txn.waiting,
            //                 wait_txn.read_set.len()
            //             );
            //             wait_txn.callback = Some(msg.callback);
            //             wait_txn.result = txn.clone();
            //         }
            //     }
            // }
            // println!("wait for {}", waiting_for_read_result);
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
                        None => break,
                    }

                    waiting_for_read_result -= 1;
                }
                msg.callback.send(Ok(txn)).await;
            });
        }
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        // let reply = YuxiMsg::default();
        // msg.callback.send(Ok(reply)).await;
        // return;
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

            let (meta_rwlock, data) = self.index.get(&key).unwrap();
            let meta = &mut meta_rwlock.write().await;
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

        // println!(
        //     "wait list tid{},{:?}",
        //     msg.tmsg.txn_id, write_ts_in_waitlist
        // );
        self.txns
            .insert(msg.tmsg.txn_id, (msg.tmsg.clone(), write_ts_in_waitlist));
        for read in msg.tmsg.read_set.iter() {
            let key = read.key;
            // find and update the ts

            let (meta_rwlock, data) = self.index.get(&key).unwrap();
            let meta = &mut meta_rwlock.write().await;
            if ts > meta.maxts {
                meta.maxts = ts;
            } else {
                if prepare_response.timestamp < meta.maxts {
                    prepare_response.timestamp = meta.maxts;
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

        // let accept_ts = msg.tmsg.timestamp;

        // let (txn, write_ts_in_waitlist) = self.txns.get(&msg.tmsg.txn_id).unwrap();

        // for (write, write_ts) in write_ts_in_waitlist.iter() {
        //     let key = write.key;

        //     let (meta_rwlock, data_index) = self.index.get(&key).unwrap();

        //     let mut to_executed = Vec::new();
        //     {
        //         let meta = &mut meta_rwlock.write().await;
        //         // let meta = &mut tuple.0;
        //         if meta.maxts < accept_ts {
        //             meta.maxts = accept_ts
        //         }

        //         if let Some(execution_context) = meta.waitlist.remove(&write_ts) {
        //             // execution_context.committed = true;
        //             meta.waitlist.insert(accept_ts, execution_context);
        //         }
        //         // check pending txns execute the context if the write is committed
        //         loop {
        //             match meta.waitlist.pop_first() {
        //                 Some((ts, context)) => {
        //                     if context.committed {
        //                         to_executed.push((ts, context));
        //                     } else {
        //                         meta.waitlist.insert(ts, context);
        //                         if meta.smallest_wait_ts < ts {
        //                             meta.smallest_wait_ts = ts;
        //                         }
        //                         break;
        //                     }
        //                 }
        //                 None => {
        //                     meta.smallest_wait_ts = MaxTs;
        //                     break;
        //                 }
        //             }
        //         }
        //     }

        //     // execute
        //     for (ts, context) in to_executed {
        //         unsafe {
        //             if context.read {
        //                 // execute the read
        //                 // get data
        //                 let datas = &DATA[*data_index];
        //                 let mut index = datas.len() - 1;
        //                 while accept_ts < datas[index].start_ts {
        //                     index -= 1;
        //                 }
        //                 let data = datas[index].data.to_string();
        //                 let (clientid, _) = get_txnid(context.txnid);
        //                 {
        //                     let mut wait_txn = WAITING_TXN[clientid as usize].write().await;
        //                     wait_txn.read_set.push(ReadStruct {
        //                         key,
        //                         value: Some(data),
        //                         timestamp: None,
        //                     });
        //                     if wait_txn.waiting == wait_txn.read_set.len() {
        //                         wait_txn
        //                             .callback
        //                             .take()
        //                             .unwrap()
        //                             .send(Ok(wait_txn.result.clone()))
        //                             .await;
        //                     }
        //                 }

        //                 // let callback = context.call_back.take().unwrap();
        //                 // callback.send((ts as i64, data));
        //             } else {
        //                 // execute the write
        //                 let datas = &mut DATA[*data_index];
        //                 match datas.last_mut() {
        //                     Some(last_data) => {
        //                         last_data.end_ts = ts;
        //                     }
        //                     None => {}
        //                 }
        //                 let mut version_data = VersionData {
        //                     start_ts: ts,
        //                     end_ts: MaxTs,
        //                     data: Data::default(),
        //                 };
        //                 match msg.tmsg.txn_type() {
        //                     rpc::common::TxnType::TatpGetSubscriberData => {}
        //                     rpc::common::TxnType::TatpGetNewDestination => {}
        //                     rpc::common::TxnType::TatpGetAccessData => {}
        //                     rpc::common::TxnType::TatpUpdateSubscriberData => {}
        //                     rpc::common::TxnType::TatpUpdateLocation => {}
        //                     rpc::common::TxnType::TatpInsertCallForwarding => {}
        //                     rpc::common::TxnType::Ycsb => {
        //                         version_data.data = Data::Ycsb(write.value.clone());
        //                     }
        //                 }
        //                 datas.push(version_data);
        //             }
        //         }
        //     }
        // }
    }

    async fn commit_write(
        &mut self,
        final_ts: u64,
        write_ts_in_waitlist: Vec<(WriteStruct, u64)>,
        tid: u64,
        txn_type: TxnType,
    ) {
        // println!("{} commit write", self.executor_id);
        for (write, write_ts) in write_ts_in_waitlist.into_iter() {
            let key = write.key;

            let (meta_rwlock, data_index) = self.index.get(&key).unwrap();

            let mut to_executed = Vec::new();
            {
                let meta = &mut meta_rwlock.write().await;
                // let meta = &mut tuple.0;
                if meta.maxts < final_ts {
                    meta.maxts = final_ts
                }

                if let Some(mut execution_context) = meta.waitlist.remove(&write_ts) {
                    execution_context.committed = true;
                    meta.waitlist.insert(final_ts, execution_context);
                }
                // check pending txns execute the context if the write is committed
                loop {
                    match meta.waitlist.pop_first() {
                        Some((ts, context)) => {
                            if context.committed {
                                to_executed.push((ts, context));
                            } else {
                                meta.waitlist.insert(ts, context);
                                if meta.smallest_wait_ts < ts {
                                    meta.smallest_wait_ts = ts;
                                }
                                break;
                            }
                        }
                        None => {
                            meta.smallest_wait_ts = MaxTs;
                            break;
                        }
                    }
                }
            }
            println!("{} commit execute context", self.executor_id);
            // execute
            for (ts, mut context) in to_executed {
                unsafe {
                    if context.read {
                        // execute the read
                        // get data

                        // let (clientid, _) = get_txnid(context.txnid);
                        // {
                        //     let mut wait_txn = WAITING_TXN[clientid as usize].write().await;
                        //     // wait_txn.waiting -= 1;
                        //     wait_txn.read_set.push(ReadStruct {
                        //         key,
                        //         value: Some(data),
                        //         timestamp: None,
                        //     });
                        //     if wait_txn.waiting == wait_txn.read_set.len() {
                        //         wait_txn
                        //             .callback
                        //             .take()
                        //             .unwrap()
                        //             .send(Ok(wait_txn.result.clone()))
                        //             .await;
                        //         println!("send back result {:?}", get_txnid(context.txnid));
                        //     }
                        // }get_data(ts, *data_index)
                        println!("index = {}", *data_index);
                        let callback = context.call_back.take().unwrap();
                        callback.send((ts as i64, get_data(ts, *data_index)));
                    } else {
                        // execute the write
                        let datas = &mut DATA[*data_index];
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
                        match txn_type {
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
                }
            }
        }
        println!("{} commit write done", self.executor_id);
    }

    async fn handle_commit(&mut self, msg: Msg) {
        // println!("{} handle commit", self.executor_id);
        let tid = msg.tmsg.txn_id;
        let final_ts = msg.tmsg.timestamp;
        let (mut txn, write_ts_in_waitlist) = self.txns.remove(&tid).unwrap();
        if self.server_id == (msg.tmsg.from as u32) % 3 {
            // unsafe {
            //     {
            //         let from = msg.tmsg.from;
            //         let mut wait_txn = WAITING_TXN[from as usize].write().await;
            //         wait_txn.waiting = 0;
            //         wait_txn.read_set.clear();
            //     }
            // }
            // get write_ts in waitlist to erase

            let txn_type = txn.txn_type();
            // execute read

            let mut waiting_for_read_result = 0;
            let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
            let mut read_set = txn.read_set.clone();
            txn.read_set.clear();
            for read in read_set.iter_mut() {
                let key = read.key;
                {
                    let (meta_rwlock, data_index) = self.index.get(&key).unwrap();
                    let meta = &mut meta_rwlock.write().await;
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
                            txnid: tid,
                        };
                        // let mut wait_list = tuple.1.write().await;
                        meta.waitlist.insert(final_ts, execution_context);
                        continue;
                    }

                    txn.read_set.push(ReadStruct {
                        key,
                        value: Some(get_data(final_ts, *data_index)),
                        timestamp: None,
                    });
                }
            }
            // println!("is reply {}, need wait?", isreply);
            // do we need to wait
            if waiting_for_read_result == 0 {
                // reply to coordinator
                msg.callback.send(Ok(txn)).await;
                // println!("send back result {:?}", get_txnid(tid));
            } else {
                // spawn a new task for this
                // unsafe {
                //     let from = txn.from;
                //     {
                //         let mut wait_txn = WAITING_TXN[from as usize].write().await;
                //         wait_txn.waiting = waiting_for_read_result;
                //         if wait_txn.waiting == wait_txn.read_set.len() {
                //             msg.callback.send(Ok(txn.clone())).await;
                //             println!("send back result {:?}", get_txnid(tid));
                //         } else {
                //             wait_txn.callback = Some(msg.callback);
                //             wait_txn.result = txn.clone();
                //         }
                //     }
                // }
                let callback = msg.callback.clone();
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
                            None => {
                                break;
                            }
                        }

                        waiting_for_read_result -= 1;
                    }
                    callback.send(Ok(txn)).await;
                });
            }
        }

        self.commit_write(final_ts, write_ts_in_waitlist, tid, msg.tmsg.txn_type())
            .await;
        // println!("{} handle commit done", self.executor_id);
    }
}
