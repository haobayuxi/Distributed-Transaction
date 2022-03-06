use common::{ReadStruct, WriteStruct};

pub struct transaction {
    pub id: i64,
    read_set: Vec<ReadStruct>,
    write_set: Vec<WriteStruct>,
    pub read_only: bool,
}

impl Default for transaction {
    fn default() -> Self {
        Self {
            id: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            read_only: false,
        }
    }
}

impl transaction {
    // pub fn new() -> Self {
    //     Self
    // }

    // pub
}
