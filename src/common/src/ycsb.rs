use std::collections::HashMap;

use rand::*;
use rpc::common::{ReadStruct, WriteStruct};

use crate::config::Config;

pub fn u64_rand(lower_bound: u64, upper_bound: u64) -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}
pub fn f64_rand(lower_bound: f64, upper_bound: f64, precision: f64) -> f64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(
        (lower_bound / precision) as u64,
        (upper_bound / precision) as u64 + 1,
    ) as f64
        * precision
}
pub enum Operation {
    Read,
    Update,
}

pub struct YcsbQuery {
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<WriteStruct>,
    zeta_2_theta: f64,
    denom: f64,
    write_value: String,
    req_per_query: i32,
    table_size: i32,
    read_perc: i32,
    theta: f64,
}

impl YcsbQuery {
    pub fn new(theta: f64, table_size: i32, req_per_query: i32, read_perc: i32) -> Self {
        let zeta_2_theta = zeta(2, theta);
        let value: Vec<char> = vec!['a'; 100];
        let mut write_value = String::from("");
        write_value.extend(value.iter());
        Self {
            read_set: Vec::new(),
            write_set: Vec::new(),
            zeta_2_theta,
            denom: zeta(table_size as u64, theta),
            write_value,
            req_per_query,
            table_size,
            read_perc,
            theta,
        }
    }

    pub fn generate(&mut self) {
        for _ in 0..self.req_per_query {
            let op = f64_rand(0.0, 1.0, 0.01);

            let key = self.zipf(self.table_size as u64, self.theta);

            if op <= self.read_perc as f64 {
                self.read_set.push(ReadStruct {
                    key: key as i64,
                    value: None,
                });
            } else {
                self.write_set.push(WriteStruct {
                    key: key as i64,
                    value: self.write_value.clone(),
                    // value: self.write_value.clone(),
                });
            }
        }
    }

    fn zipf(&self, n: u64, theta: f64) -> u64 {
        let zetan = self.denom;

        let u = f64_rand(0.0, 1.0, 0.000_000_000_000_001);
        let uz = u * zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + f64::powf(0.5, theta) {
            return 1;
        }
        let alpha = 1.0 / (1.0 - theta);

        let eta =
            (1.0 - f64::powf(2.0 / n as f64, 1.0 - theta)) / (1.0 - self.zeta_2_theta / zetan);
        let mut v = ((n as f64) * f64::powf(eta * u - eta + 1.0, alpha)) as u64;
        if v >= n {
            v = n - 1;
        }
        v
    }
}

pub fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum: f64 = 0.0;
    for i in 1..(n + 1) {
        sum += f64::powf(1.0 / (i as f64), theta);
    }
    return sum;
}

pub fn init_data(config: Config, shard_id: i32) -> HashMap<i64, String> {
    let shard_num = config.shards.len();
    let value: Vec<char> = vec!['a'; 1000];
    let mut write_value = String::from("");
    write_value.extend(value.iter());
    let mut data = HashMap::new();
    for key in 0..1000 {
        if key % shard_num == shard_id as usize {
            //
            data.insert(key as i64, write_value.clone());
        }
    }
    data
}