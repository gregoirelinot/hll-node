//! Distributed, zero-dependency HyperLogLog engine.
//!
//! * Control plane: `chitchat` gossip (UDP) — membership metadata only.
//! * Routing plane: `ArcSwap` peer snapshot + rendezvous hashing (xxh3_64).
//! * Data plane: gRPC (tonic) for clients, 1-hop forwarding, replication and handoff.

pub mod cluster;
pub mod config;
pub mod health;
pub mod hll;
pub mod metrics;
pub mod node;
pub mod peers;
pub mod router;
pub mod service;
pub mod store;

pub mod proto {
    tonic::include_proto!("hll.v1");
}

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chitchat::ChitchatHandle;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::Config;
pub use node::Node;

/// Debounce window for bursts of membership changes.
const MEMBERSHIP_DEBOUNCE: Duration = Duration::from_millis(300);

pub struct NodeHandle {
    pub node: Arc<Node>,
    pub grpc_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    chitchat: ChitchatHandle,
    shutdown: watch::Sender<bool>,
    grpc: JoinHandle<()>,
    http: JoinHandle<()>,
    background: Vec<JoinHandle<()>>,
}

async fn wait_true(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|v| *v).await;
}

/// Start a node: bind listeners, join the gossip mesh, spawn background loops.
pub async fn start(cfg: Config) -> anyhow::Result<NodeHandle> {
    cfg.validate()?;
    let node = Arc::new(Node::new(cfg)?);
    let (shutdown, shutdown_rx) = watch::channel(false);

    // Data plane (bind before we advertise ourselves via gossip).
    let grpc_listener = tokio::net::TcpListener::bind(node.cfg.grpc_listen_addr()).await?;
    let grpc_addr = grpc_listener.local_addr()?;
    let incoming = tonic::transport::server::TcpIncoming::from(grpc_listener).with_nodelay(Some(true));
    let svc = proto::hll_service_server::HllServiceServer::new(service::HllGrpc(node.clone()))
        .max_decoding_message_size(node.cfg.max_message_bytes)
        .max_encoding_message_size(node.cfg.max_message_bytes);
    let rx = shutdown_rx.clone();
    let grpc = tokio::spawn(async move {
        let res = tonic::transport::Server::builder()
            .tcp_nodelay(true)
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, wait_true(rx))
            .await;
        if let Err(e) = res {
            warn!("gRPC server error: {e}");
        }
    });

    // Health + metrics.
    let http_listener = tokio::net::TcpListener::bind(node.cfg.metrics_listen_addr()).await?;
    let metrics_addr = http_listener.local_addr()?;
    let app = health::router(node.clone());
    let rx = shutdown_rx.clone();
    let http = tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, app)
            .with_graceful_shutdown(wait_true(rx))
            .await
        {
            warn!("metrics server error: {e}");
        }
    });

    // Control plane.
    let chitchat = cluster::start(&node).await?;

    let mut background = Vec::new();

    // Membership-driven reconciler.
    let n = node.clone();
    background.push(tokio::spawn(async move {
        let mut last = n.router.membership();
        loop {
            n.membership_changed.notified().await;
            tokio::time::sleep(MEMBERSHIP_DEBOUNCE).await;
            let new = n.router.membership();
            if !new.same_routing(&last) {
                n.reconcile_membership(&last, &new).await;
                last = new;
            }
        }
    }));

    // Repair of replicas that missed deltas.
    let n = node.clone();
    background.push(tokio::spawn(async move {
        let mut tick = tokio::time::interval(n.cfg.dirty_flush_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            n.flush_dirty().await;
        }
    }));

    // Ownership GC.
    let n = node.clone();
    background.push(tokio::spawn(async move {
        let mut tick = tokio::time::interval(n.cfg.reconcile_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            n.gc_orphans().await;
        }
    }));

    // TTL sweeper.
    let n = node.clone();
    background.push(tokio::spawn(async move {
        let mut tick = tokio::time::interval(n.cfg.sweep_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            n.sweep();
        }
    }));

    // Runtime heartbeat for /healthz.
    let n = node.clone();
    background.push(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            n.heartbeat_unix_ms.store(node::now_unix_ms(), Ordering::Relaxed);
        }
    }));

    info!(%grpc_addr, %metrics_addr, node_id = %node.cfg.node_id, "hll-node started");
    Ok(NodeHandle {
        node,
        grpc_addr,
        metrics_addr,
        chitchat,
        shutdown,
        grpc,
        http,
        background,
    })
}

impl NodeHandle {
    /// Graceful shutdown (SIGTERM path):
    /// 1. gossip `state=draining` (readiness flips to 503 immediately),
    /// 2. wait for peers to drop us from HRW routing,
    /// 3. stream every local HLL to its new owners,
    /// 4. stop serving and leave the mesh.
    pub async fn drain_and_shutdown(self) {
        let node = self.node.clone();
        info!("draining");
        node.draining.store(true, Ordering::SeqCst);
        node.router.set_self_draining(true);
        cluster::set_draining(&self.chitchat).await;
        tokio::time::sleep(node.cfg.drain_wait).await;
        let left = node.handoff_all().await;
        if left > 0 {
            warn!(left, "some HLLs could not be handed off");
        }
        let _ = self.shutdown.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.grpc).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.http).await;
        if let Err(e) = self.chitchat.shutdown().await {
            warn!("chitchat shutdown: {e}");
        }
        for t in self.background {
            t.abort();
        }
        info!("shutdown complete");
    }

    /// Simulate `kill -9`: stop everything immediately without handoff.
    pub fn kill(self) {
        self.node.killed.store(true, Ordering::SeqCst);
        self.chitchat.abort();
        let _ = self.shutdown.send(true);
        self.grpc.abort();
        self.http.abort();
        for t in self.background {
            t.abort();
        }
    }
}
