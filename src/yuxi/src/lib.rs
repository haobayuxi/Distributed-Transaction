use std::collections::BTreeMap;

use common::Data;
use rpc::yuxi::YuxiMsg;
use tokio::sync::mpsc::Sender;

pub mod coordinator;
pub mod executor;
pub mod peer;
pub mod peer_communication;

pub type TS = u64;
pub type WaitList = BTreeMap<u64, i64>;
// (start ts, end ts, data)

pub struct VersionData {
    start_ts: u64,
    end_ts: u64,
    data: Data,
}

pub static MaxTs: TS = 1 << 62;

pub struct Msg {
    pub tmsg: YuxiMsg,
    pub callback: Sender<YuxiMsg>,
}
