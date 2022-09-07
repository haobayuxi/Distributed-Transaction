#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JanusMsg {
    #[prost(uint64, tag = "1")]
    pub txn_id: u64,
    #[prost(message, repeated, tag = "2")]
    pub read_set: ::prost::alloc::vec::Vec<super::common::ReadStruct>,
    #[prost(message, repeated, tag = "3")]
    pub write_set: ::prost::alloc::vec::Vec<super::common::WriteStruct>,
    #[prost(enumeration = "super::common::TxnOp", tag = "5")]
    pub op: i32,
    #[prost(uint32, tag = "6")]
    pub from: u32,
    #[prost(uint64, repeated, tag = "7")]
    pub deps: ::prost::alloc::vec::Vec<u64>,
    #[prost(enumeration = "super::common::TxnType", optional, tag = "8")]
    pub txn_type: ::core::option::Option<i32>,
}
#[doc = r" Generated client implementations."]
pub mod janus_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct JanusClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl JanusClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> JanusClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn janus_txn(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::JanusMsg>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::JanusMsg>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/janus.Janus/JanusTxn");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for JanusClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for JanusClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "JanusClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod janus_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with JanusServer."]
    #[async_trait]
    pub trait Janus: Send + Sync + 'static {
        #[doc = "Server streaming response type for the JanusTxn method."]
        type JanusTxnStream: futures_core::Stream<Item = Result<super::JanusMsg, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn janus_txn(
            &self,
            request: tonic::Request<tonic::Streaming<super::JanusMsg>>,
        ) -> Result<tonic::Response<Self::JanusTxnStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct JanusServer<T: Janus> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Janus> JanusServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for JanusServer<T>
    where
        T: Janus,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/janus.Janus/JanusTxn" => {
                    #[allow(non_camel_case_types)]
                    struct JanusTxnSvc<T: Janus>(pub Arc<T>);
                    impl<T: Janus> tonic::server::StreamingService<super::JanusMsg> for JanusTxnSvc<T> {
                        type Response = super::JanusMsg;
                        type ResponseStream = T::JanusTxnStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::JanusMsg>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).janus_txn(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = JanusTxnSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Janus> Clone for JanusServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Janus> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Janus> tonic::transport::NamedService for JanusServer<T> {
        const NAME: &'static str = "janus.Janus";
    }
}
