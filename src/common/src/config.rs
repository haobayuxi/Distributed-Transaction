use std::{collections::HashMap, vec};

pub struct MPaxosConfig {
    pub server_ids: Vec<i32>,
    pub server_addrs: HashMap<i32, String>,
}

impl Default for MPaxosConfig {
    fn default() -> Self {
        let server_ids = vec![0, 1, 2];

        let addr0 = String::from("http://192.168.50.10:20011");
        let addr1 = String::from("http://192.168.50.11:20011");
        let addr2 = String::from("http://192.168.50.12:20011");
        let mut server_addrs = HashMap::new();
        server_addrs.insert(0, addr0);
        server_addrs.insert(1, addr1);
        server_addrs.insert(2, addr2);
        Self {
            server_ids,
            server_addrs,
        }
    }
}
