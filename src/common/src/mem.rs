use std::collections::HashMap;

use crate::tatp::{AccessInfo, CallForwarding, SpecialFacility, Subscriber};

#[derive(Clone)]
pub enum Tuple {
    Ycsb(String),
    Subscriber(Subscriber),
    AccessInfo(AccessInfo),
    SpecialFacility(SpecialFacility),
    CallForwarding(CallForwarding),
}

impl Default for Tuple {
    fn default() -> Self {
        Self::Ycsb("".to_string())
    }
}

impl Tuple {
    pub fn to_string(&self) -> String {
        match self {
            Tuple::Ycsb(s) => return s.clone(),
            Tuple::Subscriber(s) => return serde_json::to_string(s).unwrap(),
            Tuple::AccessInfo(s) => return serde_json::to_string(s).unwrap(),
            Tuple::SpecialFacility(s) => return serde_json::to_string(s).unwrap(),
            Tuple::CallForwarding(s) => return serde_json::to_string(s).unwrap(),
        }
    }
}

pub struct DataStore {
    is_ycsb: bool,
    ycsb: HashMap<u64, String>,
    subscriber: HashMap<u64, Subscriber>,
    access_info: HashMap<u64, usize>,
    special_facility: HashMap<u64, usize>,
    call_forwarding: HashMap<u64, usize>,
}

impl DataStore {
    pub fn new() -> Self {
        // if is_ycsb {

        // }else {

        // }
        Self {
            is_ycsb: false,
            ycsb: HashMap::new(),
            subscriber: HashMap::new(),
            access_info: HashMap::new(),
            special_facility: HashMap::new(),
            call_forwarding: HashMap::new(),
        }
    }

    pub fn get_ycsb(&self, key: u64) -> String {
        return String::new();
    }

    pub fn set(&mut self, key: u64) {}
}

pub struct VersionedDataStore {
    is_ycsb: bool,
    ycsb: HashMap<u64, String>,
}
