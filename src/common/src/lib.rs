pub mod config;

use std::{collections::HashMap, hash::Hash};

use rpc::{
    classic::{Msgs, Txn},
    mpaxos::MPaxosMsg,
};

pub enum PeerMsg {
    Txn(Msgs),
    MPaxos(MPaxosMsg),
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

pub fn get_client_id(txnid: i64) -> i32 {
    return (txnid >> 15) as i32;
}

pub fn transaction_to_pieces(txn: Txn) -> HashMap<i32, Txn> {
    HashMap::new()
}
// pub struct ReadStruct {
//     pub key: String,
//     pub txn_id: i64,
// }

// impl ReadStruct {
//     pub fn new(key: String, txn_id: i64) -> Self {
//         Self { key, txn_id }
//     }
// }
// pub struct WriteStruct {
//     pub key: String,
//     pub value: String,
//     pub txn_id: i64,
// }

// impl WriteStruct {
//     pub fn new(key: String, value: String, txn_id: i64) -> Self {
//         Self { key, value, txn_id }
//     }
// }
