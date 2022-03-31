// use std::collections::HashMap;

// use tokio::sync::RwLock;

// #[derive(Clone)]
// pub enum Meta {
//     Tapir(TapirMeta),
//     Janus(JanusMeta),
// }

// pub struct Table {
//     pub rows: HashMap<i64, RwLock<(Meta, String)>>,
// }

// impl Table {
//     pub fn init() -> Self {
//         Self {
//             rows: HashMap::new(),
//         }
//     }
// }
