//#![feature(map_first_last)]
use std::collections::{BTreeMap, BTreeSet};

use rpc::meerkat::MeerkatMsg;
use tokio::sync::mpsc::Sender;
use tonic::Status;

pub mod coordinator;
pub mod executor;
pub mod peer;
pub mod peer_communication;

#[derive(Debug)]
pub struct Msg {
    pub tmsg: MeerkatMsg,
    pub callback: Sender<Result<MeerkatMsg, Status>>,
}

// Meerkat meta for every row
#[derive(Clone)]
pub struct MeerkatMeta {
    pub prepared_read: BTreeSet<u64>,
    pub prepared_write: BTreeSet<u64>,
    pub version: u64,
}

impl Default for MeerkatMeta {
    fn default() -> Self {
        Self {
            prepared_read: BTreeSet::new(),
            prepared_write: BTreeSet::new(),
            version: 0,
        }
    }
}
