use chrono::Local;

pub mod config;

pub static CID_LEN: i32 = 12;
pub static SHARD_NUM: i32 = 1;
pub static EXECUTOR_NUM: i32 = 16;

// remove or add http:// prefix
pub fn convert_ip_addr(ip: String, add_http: bool) -> String {
    if add_http {
        let prefix = String::from("http://");
        prefix + ip.clone().as_str()
    } else {
        let len = ip.len();
        if len <= 8 {
            return String::from("");
        }
        let result = &ip[7..len];
        result.to_string()
    }
}

pub fn get_client_id(txnid: i64) -> i32 {
    return (txnid >> CID_LEN) as i32;
}

pub fn get_local_time(coordinator_id: i32) -> i64 {
    // use microsecond as ts
    let time = Local::now().timestamp_nanos() / 1000;
    return time << CID_LEN + coordinator_id;
}
