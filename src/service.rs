//! tonic service shim: every RPC delegates to `Node`.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::node::Node;
use crate::proto::hll_service_server::HllService;
use crate::proto::*;

#[derive(Clone)]
pub struct HllGrpc(pub Arc<Node>);

impl HllGrpc {
    fn guard(&self) -> Result<&Node, Status> {
        if self.0.killed.load(Ordering::SeqCst) {
            return Err(Status::unavailable("node is shutting down"));
        }
        Ok(&self.0)
    }
}

#[tonic::async_trait]
impl HllService for HllGrpc {
    async fn add(&self, r: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
        self.guard()?.add(r.into_inner()).await.map(Response::new)
    }

    async fn count(&self, r: Request<CountRequest>) -> Result<Response<CountResponse>, Status> {
        self.guard()?.count(r.into_inner()).await.map(Response::new)
    }

    async fn merge_into(&self, r: Request<MergeIntoRequest>) -> Result<Response<MergeIntoResponse>, Status> {
        self.guard()?.merge_into(r.into_inner()).await.map(Response::new)
    }

    async fn delete(&self, r: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        self.guard()?.delete(r.into_inner()).await.map(Response::new)
    }

    async fn replicate_delta(&self, r: Request<ReplicateDeltaRequest>) -> Result<Response<Ack>, Status> {
        self.guard()?.replicate_delta(r.into_inner()).map(Response::new)
    }

    async fn push_snapshot(&self, r: Request<PushSnapshotRequest>) -> Result<Response<Ack>, Status> {
        self.guard()?.push_snapshot(r.into_inner()).map(Response::new)
    }

    async fn get_snapshot(&self, r: Request<GetSnapshotRequest>) -> Result<Response<GetSnapshotResponse>, Status> {
        self.guard()?.get_snapshot(r.into_inner()).map(Response::new)
    }
}
