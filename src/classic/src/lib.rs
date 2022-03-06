pub mod coordinator;
pub mod txn;

pub struct ReadStruct {
    pub key: String,
    pub txn_id: i64,
}

impl ReadStruct {
    pub fn new(key: String, txn_id: i64) -> Self {
        Self { key, txn_id }
    }
}
pub struct WriteStruct {
    pub key: String,
    pub value: String,
    pub txn_id: i64,
}

impl WriteStruct {
    pub fn new(key: String, value: String, txn_id: i64) -> Self {
        Self { key, value, txn_id }
    }
}
