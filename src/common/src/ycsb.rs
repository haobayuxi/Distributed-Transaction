use std::collections::HashMap;

use rand::*;

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

pub struct YCSBWorkload {
    pub read_perc: f64,
    pub write_perc: f64,
}

pub struct YcsbQuery {
    // pub read_set: Vec<ReadStr>,
    zeta_2_theta: f64,
    denom: f64,
}

impl YcsbQuery {
    pub fn new() {}

    fn generate(&mut self, config: Config, read_perc: i32, write_perc: i32) {
        for i in 0..config.req_per_query {
            let op = f64_rand(0.0, 1.0, 0.01);

            let mut key = self.zipf(config.table_size, config.zipf_theta);

            if op <= read_perc as f64 {
                self.requests[i] = YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
            } else {
                self.requests[i] =
                    YcsbRequest::new(Operation::Update, key, "0".to_string(), column);
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

pub fn init_data() -> HashMap<i64, String> {
    let mut data = HashMap::new();

    data
}
