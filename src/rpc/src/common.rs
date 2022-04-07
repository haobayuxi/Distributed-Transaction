#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadStruct {
    #[prost(int64, tag = "1")]
    pub key: i64,
    #[prost(string, optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteStruct {
    #[prost(int64, tag = "1")]
    pub key: i64,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
