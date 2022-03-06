use crate::txn::TwoPLTxn;
pub struct TwoPLCoordinator {
    id: i32,
}

impl TwoPLCoordinator {
    // get a tid
    pub fn begin_txn(&mut self, txn: TwoPLTxn) {
        let tid = 0;
    }

    pub fn prepare_txn(&mut self, txn: TwoPLTxn) {}

    pub fn commit_txn(&mut self, txn: TwoPLTxn) {}
}
