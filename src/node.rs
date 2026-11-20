//! Node core: data-plane request flows, replication, repair, reconciliation
//! and drain. The gRPC layer (`service.rs`) is a thin shim over this.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::StreamExt;
use futures::future::join_all;
use tokio::sync::{Notify, mpsc};
use tonic::Status;
use tracing::{debug, info, warn};

use crate::config::{Config, ReplicationMode};
use crate::hll::{Encoding, Hll, hash_item};
use crate::metrics::Metrics;
use crate::peers::{Client, PeerClients, is_retryable};
use crate::proto::*;
use crate::router::{Membership, Peer, Router};
use crate::store::{Origin, Store, StoreError};

pub const MAX_HLL_ID_BYTES: usize = 1024;

// ---------------------------------------------------------------------------
// Repair set: keys whose replicas may be behind and need a full snapshot push.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct DirtySet {
    map: DashMap<String, u64>,
    generation: AtomicU64,
}

impl DirtySet {
    pub fn mark(&self, key: &str) {
        let g = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        self.map.insert(key.to_owned(), g);
    }
    pub fn items(&self) -> Vec<(String, u64)> {
        self.map.iter().map(|e| (e.key().clone(), *e.value())).collect()
    }
    /// Clear only if nobody re-marked the key since `gen` was observed.
    pub fn clear_if(&self, key: &str, generation: u64) {
        self.map.remove_if(key, |_, g| *g == generation);
    }
    pub fn remove(&self, key: &str) {
        self.map.remove(key);
    }
    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Async replication: one bounded queue + batching worker per peer.
// ---------------------------------------------------------------------------

struct Delta {
    hll_id: String,
    hashes: Vec<u64>,
    ttl: u32,
    enqueued: Instant,
}

const MAX_BATCH_HASHES: usize = 256 * 1024;

async fn replication_worker(
    addr: String,
    mut rx: mpsc::Receiver<Delta>,
    peers: Arc<PeerClients>,
    dirty: Arc<DirtySet>,
    metrics: Arc<Metrics>,
    batch_interval: Duration,
) {
    while let Some(first) = rx.recv().await {
        // Coalesce per hll_id for one batching window.
        type Batch = HashMap<String, (Vec<u64>, u32, Instant)>;
        fn push(d: Delta, batch: &mut Batch) -> usize {
            let n = d.hashes.len();
            let e = batch
                .entry(d.hll_id)
                .or_insert_with(|| (Vec::new(), 0, d.enqueued));
            e.0.extend_from_slice(&d.hashes);
            e.1 = e.1.max(d.ttl);
            e.2 = e.2.min(d.enqueued);
            n
        }
        let mut batch: Batch = HashMap::new();
        let mut total = push(first, &mut batch);
        let deadline = tokio::time::Instant::now() + batch_interval;
        let mut closed = false;
        while total < MAX_BATCH_HASHES {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                d = rx.recv() => match d {
                    Some(d) => total += push(d, &mut batch),
                    None => { closed = true; break; }
                },
            }
        }

        let client = match peers.get(&addr) {
            Ok(c) => c,
            Err(_) => {
                for k in batch.keys() {
                    dirty.mark(k);
                }
                continue;
            }
        };
        futures::stream::iter(batch)
            .for_each_concurrent(32, |(hll_id, (hashes, ttl, enqueued))| {
                let mut client = client.clone();
                let dirty = dirty.clone();
                let metrics = metrics.clone();
                let addr = addr.as_str();
                async move {
                    let req = ReplicateDeltaRequest {
                        hll_id: hll_id.clone(),
                        item_hashes: hashes,
                        ttl_seconds: ttl,
                    };
                    match client.replicate_delta(req).await {
                        Ok(ack) if ack.get_ref().ok => {
                            metrics
                                .replication_lag_ms
                                .observe(enqueued.elapsed().as_secs_f64() * 1e3);
                        }
                        other => {
                            debug!(peer = %addr, %hll_id, ?other, "replicate_delta failed; scheduling snapshot repair");
                            metrics.replication_failed.inc();
                            dirty.mark(&hll_id);
                        }
                    }
                }
            })
            .await;
        if closed {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

pub struct Node {
    pub cfg: Config,
    pub store: Store,
    pub router: Router,
    pub peers: Arc<PeerClients>,
    pub metrics: Arc<Metrics>,
    pub dirty: Arc<DirtySet>,
    replicators: DashMap<String, mpsc::Sender<Delta>>,
    orphans: DashMap<String, Instant>,
    pub draining: AtomicBool,
    pub killed: AtomicBool,
    pub membership_changed: Notify,
    pub chitchat_alive: AtomicBool,
    pub heartbeat_unix_ms: AtomicU64,
}

fn store_status(e: StoreError) -> Status {
    match e {
        StoreError::ResourceExhausted => Status::resource_exhausted(
            "memory high-watermark reached; refusing to create new HLLs",
        ),
        StoreError::Tombstoned => Status::failed_precondition("hll recently deleted"),
        StoreError::Hll(e) => Status::invalid_argument(e.to_string()),
    }
}

fn validate_id(id: &str) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument("hll_id must not be empty"));
    }
    if id.len() > MAX_HLL_ID_BYTES {
        return Err(Status::invalid_argument(format!(
            "hll_id longer than {MAX_HLL_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Node {
    pub fn new(cfg: Config) -> anyhow::Result<Self> {
        let store = Store::new(
            cfg.precision,
            cfg.sparse_threshold_bytes,
            cfg.max_memory_bytes,
            cfg.tombstone_ttl.as_secs(),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let router = Router::new(cfg.node_id.as_str(), cfg.replication_factor, cfg.az_aware);
        let peers = Arc::new(PeerClients::new(cfg.rpc_timeout, cfg.max_message_bytes));
        let metrics = Arc::new(Metrics::new());
        metrics.max_memory_bytes.set(cfg.max_memory_bytes as i64);
        Ok(Self {
            cfg,
            store,
            router,
            peers,
            metrics,
            dirty: Arc::new(DirtySet::default()),
            replicators: DashMap::new(),
            orphans: DashMap::new(),
            draining: AtomicBool::new(false),
            killed: AtomicBool::new(false),
            membership_changed: Notify::new(),
            chitchat_alive: AtomicBool::new(false),
            heartbeat_unix_ms: AtomicU64::new(now_unix_ms()),
        })
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Readiness per spec: active (not draining) and enough live peers.
    pub fn is_ready(&self) -> bool {
        !self.is_draining()
            && !self.killed.load(Ordering::SeqCst)
            && self.router.membership().live_count >= self.cfg.min_quorum
    }

    pub fn is_live(&self) -> bool {
        let age = now_unix_ms().saturating_sub(self.heartbeat_unix_ms.load(Ordering::Relaxed));
        self.chitchat_alive.load(Ordering::SeqCst) && age < 10_000
    }

    /// Called by the membership watcher after the router was updated.
    pub fn on_membership_update(&self) {
        let m = self.router.membership();
        self.metrics.live_nodes.set(m.live_count as i64);
        self.metrics.active_nodes.set(m.active.len() as i64);
        let live: HashSet<String> = m.active.iter().map(|p| p.rpc_addr.clone()).collect();
        self.peers.retain(&live);
        // Dropping a sender ends that peer's replication worker.
        self.replicators.retain(|addr, _| live.contains(addr));
        self.membership_changed.notify_one();
    }

    pub fn refresh_gauges(&self) {
        let (sparse, dense) = self.store.mode_counts();
        self.metrics.keys.with_label_values(&["sparse"]).set(sparse as i64);
        self.metrics.keys.with_label_values(&["dense"]).set(dense as i64);
        self.metrics.memory_bytes.set(self.store.memory_bytes() as i64);
        self.metrics.dirty_keys.set(self.dirty.len() as i64);
    }

    // ----- helpers ---------------------------------------------------------

    /// Try each non-self owner in order; fall through on transport errors.
    async fn forward<T, F, Fut>(&self, owners: &[Arc<Peer>], mut call: F) -> Result<T, Status>
    where
        F: FnMut(Client) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let mut last = Status::unavailable("no reachable owner");
        for p in owners.iter().filter(|p| !self.router.is_self(p)) {
            let client = self.peers.get(&p.rpc_addr)?;
            match call(client).await {
                Ok(r) => return Ok(r.into_inner()),
                Err(s) if is_retryable(&s) => {
                    warn!(peer = %p.node_id, code = ?s.code(), msg = s.message(), "forward failed, trying next owner");
                    last = s;
                }
                Err(s) => return Err(s),
            }
        }
        Err(last)
    }

    fn enqueue_delta(&self, peer: &Peer, hll_id: &str, hashes: &[u64], ttl: u32) {
        let tx = self
            .replicators
            .entry(peer.rpc_addr.clone())
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(self.cfg.replication_queue_capacity);
                tokio::spawn(replication_worker(
                    peer.rpc_addr.clone(),
                    rx,
                    self.peers.clone(),
                    self.dirty.clone(),
                    self.metrics.clone(),
                    self.cfg.replication_batch_interval,
                ));
                tx
            })
            .clone();
        let d = Delta {
            hll_id: hll_id.to_owned(),
            hashes: hashes.to_vec(),
            ttl,
            enqueued: Instant::now(),
        };
        if tx.try_send(d).is_err() {
            self.metrics.replication_dropped.inc();
            self.dirty.mark(hll_id);
        }
    }

    async fn replicate(&self, replicas: &[Arc<Peer>], hll_id: &str, hashes: &[u64], ttl: u32) -> Result<(), Status> {
        let replicas: Vec<&Arc<Peer>> = replicas.iter().filter(|p| !self.router.is_self(p)).collect();
        if replicas.is_empty() {
            return Ok(());
        }
        match self.cfg.replication_mode {
            ReplicationMode::Async => {
                for p in replicas {
                    self.enqueue_delta(p, hll_id, hashes, ttl);
                }
                Ok(())
            }
            ReplicationMode::Sync => {
                let started = Instant::now();
                let calls = replicas.iter().map(|p| {
                    let req = ReplicateDeltaRequest {
                        hll_id: hll_id.to_owned(),
                        item_hashes: hashes.to_vec(),
                        ttl_seconds: ttl,
                    };
                    let client = self.peers.get(&p.rpc_addr);
                    async move {
                        let mut c = client?;
                        let ack = c.replicate_delta(req).await?;
                        if ack.get_ref().ok {
                            Ok(())
                        } else {
                            Err(Status::unavailable("replica rejected delta"))
                        }
                    }
                });
                let results: Vec<Result<(), Status>> = join_all(calls).await;
                self.metrics
                    .replication_lag_ms
                    .observe(started.elapsed().as_secs_f64() * 1e3);
                if let Some(err) = results.into_iter().find_map(Result::err) {
                    self.metrics.replication_failed.inc();
                    self.dirty.mark(hll_id);
                    return Err(Status::unavailable(format!(
                        "sync replication failed (write applied on primary, repair scheduled): {}",
                        err.message()
                    )));
                }
                Ok(())
            }
        }
    }

    pub fn snapshot_request(&self, hll_id: &str) -> Option<PushSnapshotRequest> {
        let s = self.store.snapshot(hll_id)?;
        let (enc, registers) = s.hll.to_wire();
        Some(PushSnapshotRequest {
            hll_id: hll_id.to_owned(),
            precision: s.hll.precision() as u32,
            registers,
            expires_at_unix: s.expires_at,
            encoding: enc as i32,
        })
    }

    fn decode_snapshot(&self, req: &PushSnapshotRequest) -> Result<Hll, Status> {
        let enc = Encoding::from_i32(req.encoding)
            .ok_or_else(|| Status::invalid_argument("unknown snapshot encoding"))?;
        let p = u8::try_from(req.precision).map_err(|_| Status::invalid_argument("bad precision"))?;
        if p != self.store.precision() {
            return Err(Status::invalid_argument(format!(
                "precision mismatch: local {} vs remote {p}",
                self.store.precision()
            )));
        }
        Hll::from_wire(p, enc, &req.registers, self.store.sparse_threshold_bytes())
            .map_err(|e| Status::invalid_argument(e.to_string()))
    }

    async fn push_to(&self, peer: &Peer, req: PushSnapshotRequest, reason: &str) -> bool {
        let ok = match self.peers.get(&peer.rpc_addr) {
            Ok(mut c) => matches!(c.push_snapshot(req).await, Ok(a) if a.get_ref().ok),
            Err(_) => false,
        };
        self.metrics
            .snapshots_pushed
            .with_label_values(&[reason, if ok { "ok" } else { "error" }])
            .inc();
        ok
    }

    /// Push the local copy of `hll_id` to each target; returns true if all succeeded.
    async fn push_key_to(&self, hll_id: &str, targets: &[Arc<Peer>], reason: &str) -> bool {
        let targets: Vec<&Arc<Peer>> = targets.iter().filter(|p| !self.router.is_self(p)).collect();
        if targets.is_empty() {
            return true;
        }
        let Some(req) = self.snapshot_request(hll_id) else {
            return true; // nothing local (deleted / expired)
        };
        let results = join_all(targets.iter().map(|p| self.push_to(p, req.clone(), reason))).await;
        results.into_iter().all(|ok| ok)
    }

    fn collect_hashes(item_hashes: &[u64], items: &[Vec<u8>]) -> Vec<u64> {
        let mut v = Vec::with_capacity(item_hashes.len() + items.len());
        v.extend_from_slice(item_hashes);
        v.extend(items.iter().map(|b| hash_item(b)));
        v
    }

    // ----- client-facing flows ---------------------------------------------

    pub async fn add(&self, req: AddRequest) -> Result<AddResponse, Status> {
        validate_id(&req.hll_id)?;
        let id = req.hll_id.as_str();
        let hashes = Self::collect_hashes(&req.item_hashes, &req.items);
        let owners = self.router.top_k_owners(id);

        let apply_local_fallback = |route: &str| -> Result<AddResponse, Status> {
            let modified = self
                .store
                .add(id, &hashes, req.ttl_seconds, Origin::Client)
                .map_err(store_status)?;
            // Hand the write to the real owners in the background.
            self.dirty.mark(id);
            self.metrics.op("add", route);
            Ok(AddResponse { modified })
        };

        if owners.is_empty() {
            return apply_local_fallback("loop_fallback");
        }
        if self.router.is_self(&owners[0]) {
            let modified = self
                .store
                .add(id, &hashes, req.ttl_seconds, Origin::Client)
                .map_err(store_status)?;
            self.replicate(&owners[1..], id, &hashes, req.ttl_seconds).await?;
            self.metrics.op("add", "local");
            return Ok(AddResponse { modified });
        }
        if req.hop_count >= 1 {
            // Transient view mismatch: never re-forward (1-hop guarantee).
            return apply_local_fallback("loop_fallback");
        }
        let fwd = AddRequest {
            hll_id: req.hll_id.clone(),
            item_hashes: hashes.clone(),
            ttl_seconds: req.ttl_seconds,
            hop_count: 1,
            items: Vec::new(),
        };
        match self
            .forward(&owners, |mut c| {
                let r = fwd.clone();
                async move { c.add(r).await }
            })
            .await
        {
            Ok(resp) => {
                self.metrics.op("add", "forwarded");
                Ok(resp)
            }
            // AP: every owner unreachable -> accept locally, repair later.
            Err(s) if is_retryable(&s) => apply_local_fallback("loop_fallback"),
            Err(s) => Err(s),
        }
    }

    fn count_local(&self, id: &str, route: &str) -> CountResponse {
        self.metrics.op("count", route);
        match self.store.count(id) {
            Some(c) => CountResponse { cardinality: c, exists: true },
            None => CountResponse { cardinality: 0, exists: false },
        }
    }

    pub async fn count(&self, req: CountRequest) -> Result<CountResponse, Status> {
        validate_id(&req.hll_id)?;
        let id = req.hll_id.as_str();
        let owners = self.router.top_k_owners(id);
        let local_ok = req.allow_local || self.cfg.local_reads;
        if owners.is_empty()
            || self.router.is_self(&owners[0])
            || req.hop_count >= 1
            || (local_ok && owners.iter().any(|p| self.router.is_self(p)))
        {
            return Ok(self.count_local(id, "local"));
        }
        let fwd = CountRequest {
            hll_id: req.hll_id.clone(),
            hop_count: 1,
            allow_local: false,
        };
        match self
            .forward(&owners, |mut c| {
                let r = fwd.clone();
                async move { c.count(r).await }
            })
            .await
        {
            Ok(resp) => {
                self.metrics.op("count", "forwarded");
                Ok(resp)
            }
            Err(s) if is_retryable(&s) && self.store.contains(id) => {
                Ok(self.count_local(id, "loop_fallback"))
            }
            Err(s) => Err(s),
        }
    }

    pub async fn delete(&self, req: DeleteRequest) -> Result<DeleteResponse, Status> {
        validate_id(&req.hll_id)?;
        let id = req.hll_id.as_str();
        if req.internal {
            self.dirty.remove(id);
            self.orphans.remove(id);
            return Ok(DeleteResponse { deleted: self.store.delete(id) });
        }
        let owners = self.router.top_k_owners(id);
        if owners.is_empty() || self.router.is_self(&owners[0]) || req.hop_count >= 1 {
            self.dirty.remove(id);
            self.orphans.remove(id);
            let mut deleted = self.store.delete(id);
            // Fan out to every other owner; each leaves a tombstone so that
            // in-flight deltas/snapshots cannot resurrect the key.
            let fan = owners.iter().filter(|p| !self.router.is_self(p)).map(|p| {
                let req = DeleteRequest {
                    hll_id: id.to_owned(),
                    hop_count: 1,
                    internal: true,
                };
                let client = self.peers.get(&p.rpc_addr);
                let node_id = p.node_id.clone();
                async move {
                    match client {
                        Ok(mut c) => match c.delete(req).await {
                            Ok(r) => r.into_inner().deleted,
                            Err(e) => {
                                warn!(peer = %node_id, "delete fan-out failed: {e}");
                                false
                            }
                        },
                        Err(_) => false,
                    }
                }
            });
            deleted |= join_all(fan).await.into_iter().any(|d| d);
            self.metrics.op("delete", "local");
            return Ok(DeleteResponse { deleted });
        }
        let fwd = DeleteRequest {
            hll_id: req.hll_id.clone(),
            hop_count: 1,
            internal: false,
        };
        let resp = self
            .forward(&owners, |mut c| {
                let r = fwd.clone();
                async move { c.delete(r).await }
            })
            .await?;
        self.metrics.op("delete", "forwarded");
        Ok(resp)
    }

    /// Best available copy of `hll_id`: primary first, then replicas, then local.
    async fn fetch_snapshot(&self, id: &str) -> Result<Option<(Hll, u64)>, Status> {
        let owners = self.router.top_k_owners(id);
        let local = || self.store.snapshot(id).map(|s| (s.hll, s.expires_at));
        if owners.is_empty() || self.router.is_self(&owners[0]) {
            return Ok(local());
        }
        let req = GetSnapshotRequest { hll_id: id.to_owned() };
        match self
            .forward(&owners, |mut c| {
                let r = req.clone();
                async move { c.get_snapshot(r).await }
            })
            .await
        {
            Ok(resp) => match (resp.exists, resp.snapshot) {
                (true, Some(s)) => Ok(Some((self.decode_snapshot(&s)?, s.expires_at_unix))),
                _ => Ok(None),
            },
            Err(s) if is_retryable(&s) && owners.iter().any(|p| self.router.is_self(p)) => Ok(local()),
            Err(s) => Err(s),
        }
    }

    pub async fn merge_into(&self, req: MergeIntoRequest) -> Result<MergeIntoResponse, Status> {
        validate_id(&req.dest_hll_id)?;
        if req.source_hll_ids.is_empty() {
            return Err(Status::invalid_argument("source_hll_ids must not be empty"));
        }
        for s in &req.source_hll_ids {
            validate_id(s)?;
        }
        let dest = req.dest_hll_id.as_str();
        let mut ids: Vec<&str> = req.source_hll_ids.iter().map(String::as_str).collect();
        ids.push(dest);
        ids.sort_unstable();
        ids.dedup();

        // Fetch all snapshots in parallel from their primaries.
        let fetched = join_all(ids.iter().map(|id| async move { (*id, self.fetch_snapshot(id).await) })).await;
        let mut merged = self.store.new_hll();
        let mut dest_expiry = 0u64;
        for (id, res) in fetched {
            if let Some((hll, exp)) = res? {
                merged
                    .merge(&hll)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                if id == dest {
                    dest_expiry = exp;
                }
            }
        }

        // Push the union to dest's primary + replicas.
        let owners = self.router.top_k_owners(dest);
        let (enc, registers) = merged.to_wire();
        let push = PushSnapshotRequest {
            hll_id: dest.to_owned(),
            precision: merged.precision() as u32,
            registers,
            expires_at_unix: dest_expiry,
            encoding: enc as i32,
        };
        let mut any_ok = false;
        let mut last_err = None;
        if owners.is_empty() || owners.iter().any(|p| self.router.is_self(p)) {
            match self.store.merge_snapshot(dest, &merged, dest_expiry, Origin::Client) {
                Ok(_) => any_ok = true,
                Err(e) => last_err = Some(store_status(e)),
            }
        }
        let remote: Vec<_> = owners.iter().filter(|p| !self.router.is_self(p)).collect();
        let results = join_all(remote.iter().map(|p| self.push_to(p, push.clone(), "merge"))).await;
        any_ok |= results.iter().any(|ok| *ok);
        if !any_ok {
            return Err(last_err.unwrap_or_else(|| Status::unavailable("no dest owner accepted the merge")));
        }
        if results.iter().any(|ok| !ok) && owners.iter().any(|p| self.router.is_self(p)) {
            self.dirty.mark(dest);
        }
        self.metrics.op("merge", "coordinator");
        Ok(MergeIntoResponse {
            cardinality: merged.estimate(),
        })
    }

    // ----- internal peer RPCs ---------------------------------------------

    pub fn replicate_delta(&self, req: ReplicateDeltaRequest) -> Result<Ack, Status> {
        validate_id(&req.hll_id)?;
        match self
            .store
            .add(&req.hll_id, &req.item_hashes, req.ttl_seconds, Origin::Internal)
        {
            Ok(_) => {
                self.metrics.op("replicate", "local");
                Ok(Ack { ok: true })
            }
            Err(StoreError::Tombstoned) => Ok(Ack { ok: false }),
            Err(e) => Err(store_status(e)),
        }
    }

    pub fn push_snapshot(&self, req: PushSnapshotRequest) -> Result<Ack, Status> {
        validate_id(&req.hll_id)?;
        let hll = self.decode_snapshot(&req)?;
        match self
            .store
            .merge_snapshot(&req.hll_id, &hll, req.expires_at_unix, Origin::Internal)
        {
            Ok(_) => {
                self.metrics.op("snapshot", "local");
                Ok(Ack { ok: true })
            }
            Err(StoreError::Tombstoned) => Ok(Ack { ok: false }),
            Err(e) => Err(store_status(e)),
        }
    }

    pub fn get_snapshot(&self, req: GetSnapshotRequest) -> Result<GetSnapshotResponse, Status> {
        validate_id(&req.hll_id)?;
        Ok(match self.snapshot_request(&req.hll_id) {
            Some(s) => GetSnapshotResponse { exists: true, snapshot: Some(s) },
            None => GetSnapshotResponse { exists: false, snapshot: None },
        })
    }

    // ----- background maintenance -----------------------------------------

    /// Push snapshots for keys whose replicas may be behind.
    pub async fn flush_dirty(&self) {
        let items = self.dirty.items();
        if items.is_empty() {
            return;
        }
        let m = self.router.membership();
        futures::stream::iter(items)
            .for_each_concurrent(self.cfg.handoff_concurrency, |(key, generation)| {
                let m = m.clone();
                async move {
                    let owners = self.router.owners_in(&m, &key);
                    if self.push_key_to(&key, &owners, "repair").await {
                        self.dirty.clear_if(&key, generation);
                    }
                }
            })
            .await;
        self.metrics.dirty_keys.set(self.dirty.len() as i64);
    }

    /// React to a membership transition: hand each local HLL to owners that
    /// did not own it before (new replicas after a crash, a newly joined pod,
    /// or a restarted pod with empty RAM).
    pub async fn reconcile_membership(&self, old: &Membership, new: &Membership) {
        let keys = self.store.keys();
        if keys.is_empty() {
            return;
        }
        let started = Instant::now();
        let pushed = AtomicU64::new(0);
        futures::stream::iter(keys)
            .for_each_concurrent(self.cfg.handoff_concurrency, |key| {
                let pushed = &pushed;
                async move {
                    let old_owners = self.router.owners_in(old, &key);
                    let new_owners = self.router.owners_in(new, &key);
                    let self_owner = new_owners.iter().any(|p| self.router.is_self(p));
                    let targets: Vec<Arc<Peer>> = new_owners
                        .iter()
                        .filter(|p| !self.router.is_self(p))
                        .filter(|p| !self_owner || !old_owners.iter().any(|o| o.same_instance(p)))
                        .cloned()
                        .collect();
                    if !self_owner {
                        self.orphans.entry(key.clone()).or_insert_with(Instant::now);
                    } else {
                        self.orphans.remove(&key);
                    }
                    if targets.is_empty() {
                        return;
                    }
                    pushed.fetch_add(targets.len() as u64, Ordering::Relaxed);
                    if !self.push_key_to(&key, &targets, "rebalance").await {
                        self.dirty.mark(&key);
                    }
                }
            })
            .await;
        info!(
            pushes = pushed.load(Ordering::Relaxed),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "membership reconcile done"
        );
    }

    /// Drop local copies of HLLs this node no longer owns, after a grace
    /// period and only once the handoff is confirmed (key not dirty).
    pub async fn gc_orphans(&self) {
        let m = self.router.membership();
        let grace = self.cfg.orphan_grace;
        let keys = self.store.keys();
        let present: HashSet<&String> = keys.iter().collect();
        self.orphans.retain(|k, _| present.contains(k));
        futures::stream::iter(keys.iter())
            .for_each_concurrent(self.cfg.handoff_concurrency, |key| {
                let m = m.clone();
                async move {
                    let owners = self.router.owners_in(&m, key);
                    if owners.is_empty() || owners.iter().any(|p| self.router.is_self(p)) {
                        self.orphans.remove(key);
                        return;
                    }
                    let since = self.orphans.get(key).map(|t| *t);
                    match since {
                        None => {
                            self.orphans.insert(key.clone(), Instant::now());
                            if !self.push_key_to(key, &owners, "handoff").await {
                                self.dirty.mark(key);
                            }
                        }
                        Some(t) if t.elapsed() >= grace && !self.dirty.contains(key) => {
                            if self.store.remove(key) {
                                self.metrics.gc_dropped.inc();
                            }
                            self.orphans.remove(key);
                        }
                        Some(_) => {}
                    }
                }
            })
            .await;
    }

    pub fn sweep(&self) {
        let n = self.store.sweep_expired();
        if n > 0 {
            self.metrics.evicted.inc_by(n as u64);
        }
        self.refresh_gauges();
    }

    /// Graceful-drain step 4: stream every local HLL to its new owners
    /// (which exclude this node). Returns the number of keys not handed off.
    pub async fn handoff_all(&self) -> usize {
        let keys = self.store.keys();
        let total = keys.len();
        let m = self.router.membership();
        let started = Instant::now();
        futures::stream::iter(keys)
            .for_each_concurrent(self.cfg.handoff_concurrency, |key| {
                let m = m.clone();
                async move {
                    let owners = self.router.owners_in(&m, &key);
                    if owners.is_empty() {
                        warn!(%key, "no active peer to hand off to");
                        return;
                    }
                    if !self.push_key_to(&key, &owners, "drain").await {
                        self.dirty.mark(&key);
                    }
                }
            })
            .await;
        // Retry failures (and writes that raced in via stale routing).
        for _ in 0..5 {
            if self.dirty.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            self.flush_dirty().await;
        }
        let left = self.dirty.len();
        info!(
            keys = total,
            failed = left,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "drain handoff complete"
        );
        left
    }
}

impl Peer {
    /// Same process incarnation (a restarted pod has a new generation and
    /// empty RAM, so it must be treated as a brand-new owner).
    pub fn same_instance(&self, other: &Peer) -> bool {
        self.node_id == other.node_id && self.generation_id == other.generation_id
    }
}
