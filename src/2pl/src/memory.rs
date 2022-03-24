use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

pub struct Memory {
    pub data: HashMap<String, Arc<RwLock<String>>>,
}

impl Memory {
    pub fn init_for_test() -> Self {
        let mut data = HashMap::new();

        data.insert("a".to_string(), Arc::new(RwLock::new("atest".to_string())));
        data.insert("b".to_string(), Arc::new(RwLock::new("btest".to_string())));
        Self { data }
    }

    // pub fn init()-> Self {}
}
