use std::{collections::HashMap, hash::Hash, vec};

pub static EXEC_NUM: i32 = 10;
pub static PORT: i32 = 20000;

#[derive(Clone)]
pub struct Config {
    pub server_addrs: HashMap<u32, String>,
    pub executor_num: u32,
    pub client_num: u32,
    pub req_per_query: usize,
    pub table_size: u32,
    // pub zipf_theta: f64,
    // dast config
    pub manager_ids: Vec<u32>,
    pub propose_addrs: HashMap<u32, String>,
    // pub local_nodes: Vec<Vec<i32>>,
}

// optane08 192.168.1.88
// optane09 192.168.1.89
// optane10 192.168.1.70
// optane11 192.168.1.71
// optane12 192.168.1.72
// optane13 192.168.1.73

impl Default for Config {
    fn default() -> Self {
        let addr0 = String::from("http://192.168.1.71:10051");
        let addr1 = String::from("http://192.168.1.72:10051");
        let addr2 = String::from("http://192.168.1.73:10051");
        let mut server_addrs = HashMap::new();
        server_addrs.insert(0, addr0);
        server_addrs.insert(1, addr1);
        server_addrs.insert(2, addr2);

        let propose_addr0 = String::from("http://192.168.1.71:10151");
        let propose_addr1 = String::from("http://192.168.1.72:10151");
        let propose_addr2 = String::from("http://192.168.1.73:10151");
        let mut propose_addrs = HashMap::new();
        propose_addrs.insert(0, propose_addr0);
        propose_addrs.insert(1, propose_addr1);
        propose_addrs.insert(2, propose_addr2);

        let mut shards = HashMap::new();
        shards.insert(1, vec![0, 1, 2]);
        Self {
            server_addrs,
            executor_num: 40,
            req_per_query: 1,
            table_size: 1000,
            // zipf_theta: 0.0,
            manager_ids: vec![0, 1, 2],
            propose_addrs,
            client_num: 300,
            // local_nodes: todo!(),
        }
    }
}
