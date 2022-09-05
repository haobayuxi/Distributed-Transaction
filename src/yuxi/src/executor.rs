use std::{collections::HashMap, sync::Arc};

use common::{
    tatp::{AccessInfo, CallForwarding, Subscriber},
    Data,
};
use rpc::{
    common::{ReadStruct, TxnOp},
    yuxi::YuxiMsg,
};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{peer::IN_MEMORY_DATA, ExecuteContext, MaxTs, Msg, VersionData, TS};

pub struct Executor {
    executor_id: i32,
    server_id: i32,

    replica_num: i32,
    recv: UnboundedReceiver<Msg>,
    // cache the txn in memory
    // Vec<TS> is used to index the write in waitlist
    txns: HashMap<i64, (YuxiMsg, Vec<TS>)>,
    // ycsb
    index: Arc<HashMap<i64, usize>>,
    // tatp
    subscriber: Arc<HashMap<u64, usize>>,
    access_info: Arc<HashMap<u64, usize>>,
    special_facility: Arc<HashMap<u64, usize>>,
    call_forwarding: Arc<HashMap<u64, usize>>,
}

impl Executor {
    pub fn new(
        executor_id: i32,
        server_id: i32,
        recv: UnboundedReceiver<Msg>,
        index: Arc<HashMap<i64, usize>>,
    ) -> Self {
        Self {
            executor_id,
            server_id,
            replica_num: 3,
            recv,
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
                Some(msg) => match msg.tmsg.op() {
                    TxnOp::Abort => {}
                    TxnOp::ReadOnly => self.handle_read_only(msg).await,
                    TxnOp::Commit => self.handle_commit(msg).await,
                    TxnOp::Prepare => self.handle_prepare(msg).await,
                    TxnOp::PrepareRes => {}
                    TxnOp::ReadOnlyRes => {}
                    TxnOp::Accept => self.handle_accept(msg).await,
                    TxnOp::AcceptRes => {}
                    TxnOp::CommitRes => {}
                },
                None => {}
            }
        }
    }

    async fn handle_read_only(&mut self, msg: Msg) {
        // just wait for the earlier txn to be executed
        unsafe {
            let mut txn = msg.tmsg.clone();
            let final_ts = txn.timestamp;
            let mut waiting_for_read_result = 0;
            let (sender, mut receiver) = unbounded_channel::<(i64, String)>();
            let mut read_set = txn.read_set.clone();
            txn.read_set.clear();
            for read in read_set.iter_mut() {
                let key = read.key;
                let index = self.index.get(&key).unwrap();

                let tuple = &IN_MEMORY_DATA[*index];
                let mut ts = tuple.0.write().await;
                if *ts < final_ts {
                    *ts = final_ts
                }

                if final_ts > *tuple.2.read().await {
                    // insert a read task
                    let execution_context = ExecuteContext {
                        committed: true,
                        read: true,
                        value: None,
                        call_back: Some(sender.clone()),
                    };
                    let mut wait_list = tuple.1.write().await;
                    wait_list.insert(final_ts, execution_context);
                    continue;
                }
                let version_data = &tuple.3;
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
                        let (key, value) = receiver.recv().await.unwrap();
                        txn.read_set.push(ReadStruct {
                            key,
                            value: Some(value),
                            timestamp: None,
                        });
                        waiting_for_read_result -= 1;
                    }
                    msg.callback.send(Ok(txn)).await;
                });
            }
        }
    }

    async fn handle_prepare(&mut self, msg: Msg) {
        //
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
        unsafe {
            for write in msg.tmsg.write_set.iter() {
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let mut waitlist_guard = IN_MEMORY_DATA[*index].1.write().await;
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;
                if ts > *guard {
                    *guard = ts;
                } else {
                    *guard += 1;
                    if prepare_response.timestamp < *guard {
                        prepare_response.timestamp = *guard;
                    }
                }
                // insert into wait list
                let execute_context = ExecuteContext {
                    committed: false,
                    read: false,
                    value: Some(write.value.clone()),
                    call_back: None,
                };
                waitlist_guard.insert(*guard, execute_context);
                write_ts_in_waitlist.push(*guard);
                let mut smallest_uncommitted_write_ts = IN_MEMORY_DATA[*index].2.write().await;
                if *smallest_uncommitted_write_ts > ts {
                    *smallest_uncommitted_write_ts = ts;
                }
            }
            self.txns
                .insert(msg.tmsg.txn_id, (msg.tmsg.clone(), write_ts_in_waitlist));
            for read in msg.tmsg.read_set.iter() {
                let key = read.key;
                // find and update the ts
                let index = self.index.get(&key).unwrap();
                let mut guard = IN_MEMORY_DATA[*index].0.write().await;
                if ts > *guard {
                    *guard = ts;
                } else {
                    if prepare_response.timestamp < *guard {
                        prepare_response.timestamp = *guard;
                    }
                }
            }
        }

        // send back result
        msg.callback.send(Ok(prepare_response)).await;
    }

    async fn handle_accept(&mut self, msg: Msg) {
        // update final version

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
        let (mut txn, mut write_ts_in_waitlist) = self.txns.remove(&tid).unwrap();
        let isreply = if self.server_id == (tid as i32) % self.replica_num {
            true
        } else {
            false
        };

        unsafe {
            while txn.write_set.len() > 0 {
                let write = txn.write_set.pop().unwrap();
                let key = write.key;
                let index = self.index.get(&key).unwrap();
                let tuple = &IN_MEMORY_DATA[*index];
                {
                    let mut ts = tuple.0.write().await;
                    if *ts < final_ts {
                        *ts = final_ts
                    }
                }
                // modify the wait list
                let mut wait_list = IN_MEMORY_DATA[*index].1.write().await;
                let write_ts = write_ts_in_waitlist.pop().unwrap();
                let mut execution_context = wait_list.remove(&write_ts).unwrap();
                execution_context.committed = true;
                wait_list.insert(final_ts, execution_context);
                // check pending txns execute the context if the write is committed
                loop {
                    match wait_list.pop_first() {
                        Some((ts, context)) => {
                            if context.committed {
                                if context.read {
                                    // execute the read
                                    // get data
                                    let version_data = &IN_MEMORY_DATA[*index].3;
                                    let mut index = version_data.len() - 1;
                                    while final_ts < version_data[index].start_ts {
                                        index -= 1;
                                    }
                                    let data = version_data[index].data.to_string();
                                    context.call_back.unwrap().send((ts as i64, data));
                                } else {
                                    // execute the write
                                    let mut datas = &mut IN_MEMORY_DATA[*index].3;
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
                                wait_list.insert(ts, context);
                                let mut smallest_uncommitted_write_ts =
                                    IN_MEMORY_DATA[*index].2.write().await;
                                if *smallest_uncommitted_write_ts < ts {
                                    *smallest_uncommitted_write_ts = ts;
                                }
                                break;
                            }
                        }
                        None => {
                            let mut smallest_uncommitted_write_ts =
                                IN_MEMORY_DATA[*index].2.write().await;
                            *smallest_uncommitted_write_ts = MaxTs;
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
                let index = self.index.get(&key).unwrap();

                let tuple = &IN_MEMORY_DATA[*index];
                let mut ts = tuple.0.write().await;
                if *ts < final_ts {
                    *ts = final_ts
                }

                if final_ts > *tuple.2.read().await {
                    // insert a read task
                    let execution_context = ExecuteContext {
                        committed: true,
                        read: true,
                        value: None,
                        call_back: Some(sender.clone()),
                    };
                    let mut wait_list = tuple.1.write().await;
                    wait_list.insert(final_ts, execution_context);
                    continue;
                }
                if isreply {
                    // get data
                    let version_data = &tuple.3;
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
            if isreply {
                // do we need to wait
                if waiting_for_read_result == 0 {
                    // reply to coordinator
                    msg.callback.send(Ok(txn)).await;
                } else {
                    // spawn a new task for this
                    tokio::spawn(async move {
                        while waiting_for_read_result > 0 {
                            let (key, value) = receiver.recv().await.unwrap();
                            txn.read_set.push(ReadStruct {
                                key,
                                value: Some(value),
                                timestamp: None,
                            });
                            waiting_for_read_result -= 1;
                        }
                        msg.callback.send(Ok(txn)).await;
                    });
                }
            }
            // if version_data[index].end_ts > final_ts {
            //     // safe to read
            //     read.value = Some(version_data[index].data.read());
            //     txn.read_set.push(read.clone());
            // } else {
            //     // wait for the write
            //     // let read_context = ExecuteContext {
            //     //     read: true,
            //     //     value: None,
            //     //     call_back: Some(sender.clone()),
            //     // };
            //     let mut wait_list = tuple.1.write().await;
            //     // if wait_list
            //     // insert into
            //     wait_list.insert(final_ts, read_context);
            //     waiting_for_read_result += 1;
            // }
        }
    }
}
