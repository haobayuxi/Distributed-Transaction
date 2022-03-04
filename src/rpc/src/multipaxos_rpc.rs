#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub txn_id: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MPaxosMsg {
    #[prost(int32, tag = "2")]
    pub from: i32,
    #[prost(int32, tag = "3")]
    pub ballot: i32,
    #[prost(int32, tag = "4")]
    pub index: i32,
    #[prost(message, optional, tag = "5")]
    pub txns: ::core::option::Option<super::classic::Txn>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Reply {}
#[doc = r" Generated client implementations."]
pub mod m_paxos_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct MPaxosClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MPaxosClient<tonic::transport::Channel> {
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
    impl<T> MPaxosClient<T>
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
        pub async fn m_paxos(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::MPaxosMsg>,
        ) -> Result<tonic::Response<super::Reply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/multipaxos_rpc.MPaxos/MPaxos");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for MPaxosClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for MPaxosClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MPaxosClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod m_paxos_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MPaxosServer."]
    #[async_trait]
    pub trait MPaxos: Send + Sync + 'static {
        async fn m_paxos(
            &self,
            request: tonic::Request<tonic::Streaming<super::MPaxosMsg>>,
        ) -> Result<tonic::Response<super::Reply>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MPaxosServer<T: MPaxos> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: MPaxos> MPaxosServer<T> {
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
    impl<T, B> Service<http::Request<B>> for MPaxosServer<T>
    where
        T: MPaxos,
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
                "/multipaxos_rpc.MPaxos/MPaxos" => {
                    #[allow(non_camel_case_types)]
                    struct MPaxosSvc<T: MPaxos>(pub Arc<T>);
                    impl<T: MPaxos> tonic::server::ClientStreamingService<super::MPaxosMsg> for MPaxosSvc<T> {
                        type Response = super::Reply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::MPaxosMsg>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).m_paxos(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = MPaxosSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.client_streaming(method, req).await;
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
    impl<T: MPaxos> Clone for MPaxosServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: MPaxos> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MPaxos> tonic::transport::NamedService for MPaxosServer<T> {
        const NAME: &'static str = "multipaxos_rpc.MPaxos";
    }
}
