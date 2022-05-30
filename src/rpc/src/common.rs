#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadStruct {
    #[prost(int64, tag = "1")]
    pub key: i64,
    #[prost(string, optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int64, optional, tag = "3")]
    pub timestamp: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteStruct {
    #[prost(int64, tag = "1")]
    pub key: i64,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(int64, optional, tag = "3")]
    pub timestamp: ::core::option::Option<i64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnType {
    TatpGetSubscriberData = 0,
    TatpGetNewDestination = 1,
    TatpGetAccessData = 2,
    TatpUpdateSubscriberData = 3,
    TatpUpdateLocation = 4,
    TatpInsertCallForwarding = 5,
    Ycsb = 6,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnOp {
    ReadOnly = 0,
    Prepare = 1,
    Accept = 2,
    Commit = 3,
    ReadOnlyRes = 4,
    PrepareRes = 5,
    AcceptRes = 6,
    CommitRes = 7,
    Abort = 8,
}
