use chrono::Local;
use serde::{Deserialize, Serialize};

pub mod config;
pub mod mem;
pub mod tatp;
pub mod ycsb;

pub static CID_LEN: u32 = 50;
pub static EXECUTOR_NUM: u32 = 16;

#[derive(Default, Deserialize, Clone, Copy)]
pub struct ConfigInFile {
    pub id: u32,
    pub read_perc: i32,
    pub txns_per_client: i32,
    pub client_num: u32,
    pub is_ycsb: bool,
    pub zipf: f64,
}

// remove or add http:// prefix
pub fn convert_ip_addr(ip: String, add_http: bool) -> String {
    if add_http {
        let prefix = String::from("http://");
        prefix + ip.clone().as_str()
    } else {
        let len = ip.len();
        if len <= 8 {
            return String::from("");
        }
        let result = &ip[7..len];
        result.to_string()
    }
}

pub fn get_client_id(txnid: u64) -> u32 {
    return (txnid >> CID_LEN) as u32;
}

pub fn get_txnid(txnid: u64) -> (u64, u64) {
    let cid = get_client_id(txnid) as u64;
    let tid = txnid - (cid << CID_LEN);
    (cid, tid)
}

pub fn get_local_time(coordinator_id: u32) -> u64 {
    // use microsecond as ts
    let time = (Local::now().timestamp_nanos() / 1000) as u64;
    return time << 10 + coordinator_id;
}
