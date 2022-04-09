//#![feature(map_first_last)]
use std::collections::{BTreeMap, BTreeSet};

use rpc::tapir::TapirMsg;
use tokio::sync::mpsc::Sender;

pub mod coordinator;
pub mod executor;
pub mod peer_communication;

pub struct Msg {
    pub tmsg: TapirMsg,
    pub callback: Sender<TapirMsg>,
}

// tapir meta for every row
#[derive(Clone)]
pub struct TapirMeta {
    pub prepared_read: BTreeSet<i64>,
    pub prepared_write: BTreeSet<i64>,
    pub version: i64,
}

impl Default for TapirMeta {
    fn default() -> Self {
        Self {
            prepared_read: BTreeSet::new(),
            prepared_write: BTreeSet::new(),
            version: 0,
        }
    }
}
