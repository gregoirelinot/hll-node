//! Persistent, lazily-connected HTTP/2 channels to peers (one per rpc_addr).

use std::collections::HashSet;
use std::time::Duration;

use dashmap::DashMap;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use crate::proto::hll_service_client::HllServiceClient;

pub type Client = HllServiceClient<Channel>;

pub struct PeerClients {
    clients: DashMap<String, Client>,
    timeout: Duration,
    max_message_bytes: usize,
}

impl PeerClients {
    pub fn new(timeout: Duration, max_message_bytes: usize) -> Self {
        Self {
            clients: DashMap::new(),
            timeout,
            max_message_bytes,
        }
    }

    pub fn get(&self, addr: &str) -> Result<Client, Status> {
        if let Some(c) = self.clients.get(addr) {
            return Ok(c.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| Status::internal(format!("bad peer address {addr}: {e}")))?
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true);
        let client = HllServiceClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes);
        Ok(self
            .clients
            .entry(addr.to_owned())
            .or_insert(client)
            .clone())
    }

    /// Drop channels to peers that are no longer part of the cluster.
    pub fn retain(&self, live_addrs: &HashSet<String>) {
        self.clients.retain(|addr, _| live_addrs.contains(addr));
    }
}

/// Errors worth retrying against the next owner (transport-level failures).
pub fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::Aborted
    )
}
