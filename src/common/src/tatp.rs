use serde::{Deserialize, Serialize};

use crate::ycsb::{f64_rand, u64_rand};

#[derive(Serialize, Deserialize, Clone)]
pub struct Subscriber {
    pub s_id: u64,
    pub sub_nbr: String,

    pub bit_1: bool,
    pub bit_2: bool,
    pub bit_3: bool,
    pub bit_4: bool,
    pub bit_5: bool,
    pub bit_6: bool,
    pub bit_7: bool,
    pub bit_8: bool,
    pub bit_9: bool,
    pub bit_10: bool,

    pub hex_1: u8,
    pub hex_2: u8,
    pub hex_3: u8,
    pub hex_4: u8,
    hex_5: u8,
    hex_6: u8,
    hex_7: u8,
    hex_8: u8,
    hex_9: u8,
    hex_10: u8,

    bytes2_1: u8,
    bytes2_2: u8,
    bytes2_3: u8,
    bytes2_4: u8,
    bytes2_5: u8,
    bytes2_6: u8,
    bytes2_7: u8,
    bytes2_8: u8,
    bytes2_9: u8,
    bytes2_10: u8,

    pub msc_location: u32,
    pub vlr_location: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AccessInfo {
    pub s_id: u64,
    pub ai_type: u8,
    pub data1: u8,
    pub data2: u8,
    data3: String,
    data4: String,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct CallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub numberx: String,
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

pub fn init_tatp_data() {}

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
