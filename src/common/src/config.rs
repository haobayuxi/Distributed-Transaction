use std::{collections::HashMap, hash::Hash, vec};

pub static EXEC_NUM: i32 = 10;
pub static PORT: i32 = 20000;

#[derive(Clone)]
pub struct Config {
    pub server_ids: Vec<i32>,
    pub server_addrs: HashMap<i32, String>,
    pub executor_num: i32,
    pub shards: HashMap<i32, Vec<i32>>,
    pub req_per_query: usize,
    pub table_size: u64,
    pub zipf_theta: f64,
}

impl Default for Config {
    fn default() -> Self {
        let server_ids = vec![0, 1, 2];

        let addr0 = String::from("http://192.168.50.10:20011");
        let addr1 = String::from("http://192.168.50.11:20011");
        let addr2 = String::from("http://192.168.50.12:20011");
        let mut server_addrs = HashMap::new();
        server_addrs.insert(0, addr0);
        server_addrs.insert(1, addr1);
        server_addrs.insert(2, addr2);

        let mut shards = HashMap::new();
        shards.insert(1, server_ids.clone());
        Self {
            server_ids,
            server_addrs,
            executor_num: 16,
            shards,
            req_per_query: 10,
            table_size: 1000,
            zipf_theta: 0.0,
        }
    }
}
