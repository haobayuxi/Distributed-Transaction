use common::{ReadStruct, WriteStruct};

pub struct TwoPLTxn {
    pub id: i64,
    read_set: Vec<ReadStruct>,
    write_set: Vec<WriteStruct>,
    pub read_only: bool,
}

impl TwoPLTxn {}
