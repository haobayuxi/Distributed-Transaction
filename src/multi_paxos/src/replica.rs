use std::collections::HashMap;

pub enum Role {
    Leader,
    Follower,
}

pub struct Replica {
    role: Role,
    log_index: i32,
    // peers: HashMap<i32, UnboundedSender>,
}
impl Replica {
    // pub fn new()->Self{
    //     Self{

    //     }
    // }

    pub async fn put(&mut self) {
        let index = self.log_index;
        self.log_index += 1;
    }
}
