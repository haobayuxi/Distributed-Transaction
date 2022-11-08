#![feature(map_first_last)]
use std::collections::BTreeMap;

use common::mem::Tuple;
use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tonic::Status;

pub mod coordinator;
pub mod executor;
pub mod peer;
pub mod peer_communication;

pub type TS = u64;
pub type WaitList = BTreeMap<u64, ExecuteContext>;
// (start ts, end ts, data)

pub struct VersionData {
    start_ts: u64,
    end_ts: u64,
    data: Tuple,
}

#[derive(Debug)]
pub struct ExecuteContext {
    txnid: u64,
    committed: bool,
    read: bool,
    value: Option<String>,
    call_back: Option<UnboundedSender<usize>>,
}

pub static MaxTs: TS = 1 << 62;

#[derive(Debug)]
pub struct Msg {
    pub tmsg: YuxiMsg,
    pub callback: Sender<Result<YuxiMsg, Status>>,
}
