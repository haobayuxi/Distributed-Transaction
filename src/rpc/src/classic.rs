#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operation {
    #[prost(enumeration = "OpType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Txn {
    #[prost(int64, tag = "1")]
    pub txn_id: i64,
    #[prost(message, repeated, tag = "2")]
    pub ops: ::prost::alloc::vec::Vec<Operation>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OpType {
    Write = 0,
    Insert = 1,
    Read = 2,
}
