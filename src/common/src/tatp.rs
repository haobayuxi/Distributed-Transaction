use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
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

pub struct AccessInfo {
    pub s_id: u64,
    pub ai_type: u8,
    pub data1: u8,
    pub data2: u8,
    data3: String,
    data4: String,
}

pub struct SpecialFacility {
    pub s_id: u64,
    pub sf_type: u8,
    pub is_active: bool,
    error_contrl: u8,
    pub data_a: u8,
    data_b: String,
}

pub struct CallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub numberx: String,
}

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
