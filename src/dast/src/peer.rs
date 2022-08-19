pub struct Peer {
    id: i32,
    readyq: Arc<RwLock<Vec<u64>>>,
    waitq: Arc<RwLock<Vec<u64>>>,
}

impl Peer {
    pub fn create_ts(&mut self) -> u64 {}

    // fn CoordIRT(&mut self, txn)
}
