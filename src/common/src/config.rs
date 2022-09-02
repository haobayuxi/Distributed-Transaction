use std::{collections::HashMap, hash::Hash, vec};

pub static EXEC_NUM: i32 = 10;
pub static PORT: i32 = 20000;

#[derive(Clone)]
pub struct Config {
    // map serverid to shard id
    pub server_ids: HashMap<i32, i32>,
    pub server_addrs: HashMap<i32, String>,
    pub executor_num: i32,
    pub shards: HashMap<i32, Vec<i32>>,
    pub req_per_query: usize,
    pub table_size: i32,
    pub zipf_theta: f64,
    // dast config
    pub manager_ids: Vec<i32>,
    pub propose_addrs: HashMap<i32, String>,
    // pub local_nodes: Vec<Vec<i32>>,
}

impl Default for Config {
    fn default() -> Self {
        let mut server_ids = HashMap::new();
        server_ids.insert(0, 1);
        server_ids.insert(1, 1);
        server_ids.insert(2, 1);

        let addr0 = String::from("http://192.168.50.10:50051");
        let addr1 = String::from("http://192.168.50.11:50051");
        let addr2 = String::from("http://192.168.50.12:50051");
        let mut server_addrs = HashMap::new();
        server_addrs.insert(0, addr0);
        server_addrs.insert(1, addr1);
        server_addrs.insert(2, addr2);

        let propose_addr0 = String::from("http://192.168.50.10:50051");
        let propose_addr1 = String::from("http://192.168.50.11:50051");
        let propose_addr2 = String::from("http://192.168.50.12:50051");
        let mut propose_addrs = HashMap::new();
        propose_addrs.insert(0, propose_addr0);
        propose_addrs.insert(1, propose_addr1);
        propose_addrs.insert(2, propose_addr2);

        let mut shards = HashMap::new();
        shards.insert(1, vec![0, 1, 2]);
        Self {
            server_ids,
            server_addrs,
            executor_num: 16,
            shards,
            req_per_query: 10,
            table_size: 1000,
            zipf_theta: 0.0,
            manager_ids: vec![0, 1, 2],
            propose_addrs,
            // local_nodes: todo!(),
        }
    }
}
