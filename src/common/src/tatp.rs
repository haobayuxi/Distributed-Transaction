use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::ycsb::{f64_rand, u64_rand};

pub static mut SUBSCRIBER_FIELDS: Vec<&str> = Vec::new();
pub static mut ACCESS_INFO_FIELDS: Vec<&str> = Vec::new();
pub static mut SPECIAL_FACILITY_FIELDS: Vec<&str> = Vec::new();
pub static mut CALL_FORWARDING_FIELDS: Vec<&str> = Vec::new();

pub fn init_tatp_const() {
    unsafe {
        SUBSCRIBER_FIELDS = vec![
            "s_id",
            "sub_nbr",
            "bit_1",
            "bit_2",
            "bit_3",
            "bit_4",
            "bit_5",
            "bit_6",
            "bit_7",
            "bit_8",
            "bit_9",
            "bit_10",
            "hex_1",
            "hex_2",
            "hex_3",
            "hex_4",
            "hex_5",
            "hex_6",
            "hex_7",
            "hex_8",
            "hex_9",
            "hex_10",
            "bytes2_1",
            "bytes2_2",
            "bytes2_3",
            "bytes2_4",
            "bytes2_5",
            "bytes2_6",
            "bytes2_7",
            "bytes2_8",
            "bytes2_9",
            "bytes2_10",
            "msc_location",
            "vlr_location",
        ];
        ACCESS_INFO_FIELDS = vec!["s_id", "ai_type", "data1", "data2", "data3", "data4"];

        SPECIAL_FACILITY_FIELDS = vec![
            "s_id",
            "sf_type",
            "is_active",
            "error_cntrl",
            "data_a",
            "data_b",
        ];

        CALL_FORWARDING_FIELDS = vec!["s_id", "sf_type", "start_time", "end_time", "numberx"];
    }
}

pub fn rnd(param: &str) -> i64 {
    if param.starts_with("msc_location") || param.starts_with("vlr_location") {
        return u64_rand(0, 10000000) as i64;
    } else if param.starts_with("bit") {
        return u64_rand(0, 1) as i64;
    } else if param.starts_with("is_active") {
        let i = u64_rand(0, 99);
        if i <= 14 {
            return 0;
        } else {
            return 1;
        }
    } else if param.starts_with("hex") {
        return u64_rand(0, 15) as i64;
    } else if param.starts_with("byte") || param.starts_with("data") {
        return u64_rand(0, 255) as i64;
    } else if param.starts_with("start_time") {
        return (u64_rand(0, 2) * 8) as i64;
    } else if param.eq("end_time") {
        return (u64_rand(0, 2) * 8 + u64_rand(0, 7)) as i64;
    } else if param.eq("end_time") {
        return u64_rand(1, 8) as i64;
    } else if param.starts_with("ai_type") || param.starts_with("sf_type") {
        return u64_rand(1, 4) as i64;
    }
    0
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Subscriber {
    pub s_id: u64,
    pub sub_nbr: String,
    bit: Vec<bool>,
    // pub bit_1: bool,
    // pub bit_2: bool,
    // pub bit_3: bool,
    // pub bit_4: bool,
    // pub bit_5: bool,
    // pub bit_6: bool,
    // pub bit_7: bool,
    // pub bit_8: bool,
    // pub bit_9: bool,
    // pub bit_10: bool,
    hex: Vec<u8>,
    // pub hex_1: u8,
    // pub hex_2: u8,
    // pub hex_3: u8,
    // pub hex_4: u8,
    // hex_5: u8,
    // hex_6: u8,
    // hex_7: u8,
    // hex_8: u8,
    // hex_9: u8,
    // hex_10: u8,
    bytes: Vec<u8>,
    // bytes2_1: u8,
    // bytes2_2: u8,
    // bytes2_3: u8,
    // bytes2_4: u8,
    // bytes2_5: u8,
    // bytes2_6: u8,
    // bytes2_7: u8,
    // bytes2_8: u8,
    // bytes2_9: u8,
    // bytes2_10: u8,
    pub msc_location: u32,
    pub vlr_location: u32,
}

impl Subscriber {
    pub fn new(s_id: u64) -> Self {
        let a: u16 = s_id as u16;
        let mut bit = Vec::new();
        let mut hex = Vec::new();
        let mut bytes = Vec::new();
        for i in 0..10 {
            bit.push(rnd("bit") != 0);
            hex.push(rnd("hex") as u8);
            bytes.push(rnd("bytes") as u8);
        }
        Self {
            s_id,
            sub_nbr: a.to_string(),
            bit,
            hex,
            bytes,
            msc_location: rnd("msc_location") as u32,
            vlr_location: rnd("vlr_location") as u32,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AccessInfo {
    pub s_id: u64,
    pub ai_type: u8,
    // data: Vec<u8>,
    pub data1: u8,
    pub data2: u8,
    data3: String,
    data4: String,
}

impl AccessInfo {
    pub fn new(s_id: u64) -> Self {
        Self {
            s_id,
            ai_type: rnd("ai_type") as u8,
            data1: rnd("data") as u8,
            data2: rnd("data") as u8,
            data3: "ABC".to_string(),
            data4: "ABCDE".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SpecialFacility {
    pub s_id: u64,
    pub sf_type: u8,
    pub is_active: bool,
    error_contrl: u8,
    pub data_a: u8,
    data_b: String,
}

impl SpecialFacility {
    pub fn new(s_id: u64, sf_type: u8) -> Self {
        Self {
            s_id,
            sf_type,
            is_active: rnd("is_active") != 0,
            error_contrl: 0,
            data_a: rnd("data") as u8,
            data_b: "ABCDE".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub numberx: String,
}

impl CallForwarding {
    pub fn new(s_id: u64, sf_type: u8, start_time: u8, end_time: u8) -> Self {
        Self {
            s_id,
            sf_type,
            start_time,
            end_time,
            numberx: "ABCD".to_string(),
        }
    }
}

#[derive(Default)]
pub struct GetSubscriberDataQuery {
    pub s_id: u64,
}

pub struct GetNewDestinationQuery {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
}

pub struct GetAccessDataQuery {
    pub s_id: u64,
    pub ai_type: u8,
}

pub struct UpdateSubscriberDataQuery {
    pub s_id: u64,
    pub sf_type: u8,
    pub data_a: u8,
}

pub struct UpdateLocationQuery {
    pub sub_nbr: String,
    pub vlr_location: u32,
}

pub struct InsertCallForwardingQuery {
    pub sub_nbr: String,
    pub sf_type: u8,
    pub start_time: u8,
    pub entd_time: u8,
}

pub enum TatpQuery {
    GetSubscriberData(GetSubscriberDataQuery),
    GetNewDestination(GetNewDestinationQuery),
    GetAccessData(GetAccessDataQuery),
    UpdateSubscriberData(UpdateSubscriberDataQuery),
    UpdateLocation(UpdateLocationQuery),
    InsertCallForwarding(InsertCallForwardingQuery),
}

/**
 * subscriber: Arc<HashMap<u64, Subscriber>>,
    access_info: Arc<HashMap<u64, AccessInfo>>,
    special_facility: Arc<HashMap<u64, AccessInfo>>,
    call_forwarding: Arc<HashMap<u64, CallForwarding>>,
*/

pub fn init_tatp_data() -> (
    Arc<HashMap<u64, Subscriber>>,
    Arc<HashMap<u64, AccessInfo>>,
    Arc<HashMap<u64, SpecialFacility>>,
    Arc<HashMap<u64, CallForwarding>>,
) {
    let mut subscriber: HashMap<u64, Subscriber> = HashMap::new();
    let mut access_info: HashMap<u64, AccessInfo> = HashMap::new();
    let mut special_facility: HashMap<u64, SpecialFacility> = HashMap::new();
    let mut call_forwarding: HashMap<u64, CallForwarding> = HashMap::new();

    let start_times = vec![0, 8, 16];

    for s_id in 0..65535 {
        // init subscriber
        let subscriber_record = Subscriber::new(s_id);
        subscriber.insert(s_id, subscriber_record);
        // init access_info
        let access_info_record = AccessInfo::new(s_id);
        access_info.insert(s_id, access_info_record);
        // init special_facility record
        let subrecord_amount = u64_rand(1, 4);
        let mut sr_type = vec![0; 4];
        for i in 0..subrecord_amount {
            loop {
                let sr = rnd("ai_type") as usize;
                if sr_type[sr] == 0 {
                    // insert special_facility record
                    let special_facility_record = SpecialFacility::new(s_id, sr as u8);
                    special_facility.insert(s_id, special_facility_record);

                    // init call_forwarding record
                    let callfw_amount = u64_rand(0, 3);
                    for j in 0..callfw_amount {
                        let start_time = start_times[j as usize];
                        let end_time = start_time + u64_rand(1, 8);
                        let call_forwarding_record =
                            CallForwarding::new(s_id, sr as u8, start_time as u8, end_time as u8);
                        call_forwarding.insert(s_id, call_forwarding_record);
                    }

                    sr_type[sr] = 1;
                    break;
                }
            }
        }
    }

    return (
        Arc::new(subscriber),
        Arc::new(access_info),
        Arc::new(special_facility),
        Arc::new(call_forwarding),
    );
}

fn tatp_random(x: u64, y: u64) -> u64 {
    return ((u64_rand(0, 65535)) | (u64_rand(x, y))) % (y - x + 1) + x;
}
pub struct TatpWorkload {
    subscriber_rows: u64,
}

impl TatpWorkload {
    pub fn generate(&mut self) -> TatpQuery {
        let sid = tatp_random(1, self.subscriber_rows);
        let op = f64_rand(0.0, 1.0, 0.01);
        if op * 100.0 < 35 as f64 {
            //
        } else if op * 100.0 < 45 as f64 {
            //
        } else if op * 100.0 < 80 as f64 {
            //
        }
        return TatpQuery::GetAccessData(GetAccessDataQuery {
            s_id: 0,
            ai_type: todo!(),
        });
    }
}
