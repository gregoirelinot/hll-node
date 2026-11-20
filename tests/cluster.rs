//! In-process multi-node integration tests: real chitchat over UDP and real
//! gRPC over TCP on 127.0.0.1. These are the local stand-ins for the EKS
//! acceptance/chaos plan (kill -9 failover, rolling drain, scale up/down).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hll_node::hll::{Hll, hash_item};
use hll_node::proto::hll_service_client::HllServiceClient;
use hll_node::proto::*;
use hll_node::{Config, NodeHandle};
use tonic::transport::Channel;

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn test_config(name: &str, seed: Option<u16>) -> (Config, u16) {
    let gossip = free_udp_port();
    let mut c = Config::local(name, gossip, free_tcp_port(), free_tcp_port());
    c.cluster_id = "test".into();
    c.seed_nodes = seed.map(|p| vec![format!("127.0.0.1:{p}")]).unwrap_or_default();
    c.gossip_interval = Duration::from_millis(100);
    c.fd_initial_interval = Duration::from_millis(100);
    c.phi_threshold = 6.0;
    c.dead_node_grace_period = Duration::from_secs(600);
    c.rpc_timeout = Duration::from_millis(800);
    c.replication_batch_interval = Duration::from_millis(10);
    c.drain_wait = Duration::from_millis(600);
    c.orphan_grace = Duration::from_secs(2);
    c.reconcile_interval = Duration::from_millis(500);
    c.dirty_flush_interval = Duration::from_millis(200);
    c.tombstone_ttl = Duration::from_secs(120);
    (c, gossip)
}

async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

struct Cluster {
    nodes: Vec<Option<NodeHandle>>,
    seed_port: u16,
}

impl Cluster {
    async fn start(n: usize) -> Self {
        let mut c = Cluster { nodes: Vec::new(), seed_port: 0 };
        for i in 0..n {
            c.add_node(i).await;
        }
        c.wait_active(n).await;
        c
    }

    async fn add_node(&mut self, i: usize) {
        let seed = (i > 0).then_some(self.seed_port);
        let (cfg, gossip) = test_config(&format!("hll-node-{i}"), seed);
        if i == 0 {
            self.seed_port = gossip;
        }
        let h = hll_node::start(cfg).await.expect("start node");
        if self.nodes.len() <= i {
            self.nodes.resize_with(i + 1, || None);
        }
        self.nodes[i] = Some(h);
    }

    fn node(&self, i: usize) -> &NodeHandle {
        self.nodes[i].as_ref().expect("node alive")
    }

    fn alive(&self) -> impl Iterator<Item = &NodeHandle> {
        self.nodes.iter().flatten()
    }

    async fn wait_active(&self, n: usize) {
        wait_until(&format!("{n} active nodes everywhere"), Duration::from_secs(20), || async {
            self.alive().all(|h| h.node.router.membership().active.len() == n)
        })
        .await;
        // Let the debounced reconciler observe the stable view.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    async fn client(&self, i: usize) -> HllServiceClient<Channel> {
        HllServiceClient::connect(format!("http://{}", self.node(i).grpc_addr))
            .await
            .unwrap()
    }

    /// Node indices currently owning `id` (per node 0's... or any alive node's view).
    fn owner_names(&self, id: &str) -> Vec<String> {
        let h = self.alive().next().unwrap();
        h.node.router.top_k_owners(id).iter().map(|p| p.node_id.to_string()).collect()
    }

    fn holders(&self, id: &str) -> Vec<String> {
        self.alive()
            .filter(|h| h.node.store.contains(id))
            .map(|h| h.node.cfg.node_id.clone())
            .collect()
    }

    async fn shutdown(self) {
        for h in self.nodes.into_iter().flatten() {
            h.kill();
        }
    }
}

fn items(key: &str, range: std::ops::Range<u64>) -> Vec<u64> {
    range.map(|i| hash_item(format!("{key}/{i}").as_bytes())).collect()
}

fn reference(hashes: &[u64]) -> Hll {
    let mut h = Hll::new(14, hll_node::hll::DEFAULT_SPARSE_THRESHOLD_BYTES).unwrap();
    h.insert_hashes(hashes);
    h
}

async fn add(c: &mut HllServiceClient<Channel>, id: &str, hashes: Vec<u64>) {
    c.add(AddRequest {
        hll_id: id.into(),
        item_hashes: hashes,
        ..Default::default()
    })
    .await
    .unwrap();
}

async fn count(c: &mut HllServiceClient<Channel>, id: &str) -> CountResponse {
    c.count(CountRequest {
        hll_id: id.into(),
        ..Default::default()
    })
    .await
    .unwrap()
    .into_inner()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// Write via one node, read via another, verify accuracy and that both
/// replicas hold a copy; then kill -9 a node and verify zero data loss and
/// restoration of the replication factor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_replication_and_kill9_failover() {
    let cluster = Cluster::start(3).await;
    let mut c0 = cluster.client(0).await;
    let mut c2 = cluster.client(2).await;

    let keys: Vec<String> = (0..120).map(|i| format!("hll-{i}")).collect();
    let mut truth: HashMap<String, u64> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
        let n = 500 + (i as u64 * 97) % 5000;
        let hs = items(k, 0..n);
        for chunk in hs.chunks(1000) {
            add(&mut c0, k, chunk.to_vec()).await;
        }
        truth.insert(k.clone(), n);
    }

    // Reads from a different node; accuracy vs ground truth.
    let mut before = HashMap::new();
    let mut err_sum = 0.0;
    for k in &keys {
        let r = count(&mut c2, k).await;
        assert!(r.exists, "{k} missing");
        let t = truth[k] as f64;
        let err = (r.cardinality as f64 - t).abs() / t;
        assert!(err < 0.04, "{k}: est {} truth {t}", r.cardinality);
        err_sum += err;
        // Exact match with a local reference sketch over the same hashes.
        assert_eq!(r.cardinality, reference(&items(k, 0..truth[k])).estimate());
        before.insert(k.clone(), r.cardinality);
    }
    let mean_err = err_sum / keys.len() as f64;
    assert!(mean_err < 0.01, "mean relative error {mean_err}");

    // Both owners hold a copy (async replication settles quickly).
    wait_until("R=2 copies of every key", Duration::from_secs(10), || async {
        keys.iter().all(|k| sorted(cluster.holders(k)) == sorted(cluster.owner_names(k)))
    })
    .await;

    // Forwarding happened (client hit node 0 for keys owned elsewhere).
    let fwd = cluster.node(0).node.metrics.ops.with_label_values(&["add", "forwarded"]).get();
    assert!(fwd > 0, "expected forwarded adds");

    // kill -9 node 1.
    let mut cluster = cluster;
    let victim = cluster.nodes[1].take().unwrap();
    victim.kill();
    cluster.wait_active(2).await;

    // Zero data loss: every count identical to before the crash.
    let mut c0 = cluster.client(0).await;
    for k in &keys {
        let r = count(&mut c0, k).await;
        assert!(r.exists, "{k} lost after failover");
        assert_eq!(r.cardinality, before[k], "{k} changed after failover");
    }

    // Replication factor restored on survivors.
    wait_until("R=2 restored after failover", Duration::from_secs(15), || async {
        keys.iter().all(|k| cluster.holders(k).len() == 2)
    })
    .await;

    // Writes keep working after failover.
    add(&mut c0, "post-crash", items("post-crash", 0..3000)).await;
    let r = count(&mut c2, "post-crash").await;
    assert_eq!(r.cardinality, reference(&items("post-crash", 0..3000)).estimate());

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_does_not_resurrect() {
    let cluster = Cluster::start(3).await;
    let mut c0 = cluster.client(0).await;
    let id = "to-delete";
    add(&mut c0, id, items(id, 0..5000)).await;
    wait_until("replicated", Duration::from_secs(5), || async {
        cluster.holders(id).len() == 2
    })
    .await;

    // Delete via a non-primary node.
    let owners = cluster.owner_names(id);
    let non_primary = (0..3)
        .find(|i| cluster.node(*i).node.cfg.node_id != owners[0])
        .unwrap();
    let mut cn = cluster.client(non_primary).await;
    let r = cn
        .delete(DeleteRequest { hll_id: id.into(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert!(r.deleted);

    // Give repair / reconcile loops several rounds to (wrongly) resurrect it.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(cluster.holders(id).is_empty(), "resurrected on {:?}", cluster.holders(id));
    for i in 0..3 {
        let mut c = cluster.client(i).await;
        assert!(!count(&mut c, id).await.exists);
    }

    // A fresh client write after delete recreates it from scratch.
    add(&mut c0, id, items("fresh", 0..100)).await;
    let r = count(&mut c0, id).await;
    assert_eq!(r.cardinality, reference(&items("fresh", 0..100)).estimate());
    cluster.shutdown().await;
}

/// Rolling-restart style: drain a node under continuous write load; no
/// client RPC may fail and no data may be lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_drain_under_load() {
    let mut cluster = Cluster::start(3).await;
    let keys: Vec<String> = (0..60).map(|i| format!("drain-{i}")).collect();
    let mut c0 = cluster.client(0).await;
    for k in &keys {
        add(&mut c0, k, items(k, 0..2000)).await;
    }

    // Continuous writer against node 0 while node 2 drains.
    let writer_keys = keys.clone();
    let mut wc = cluster.client(0).await;
    let writer = tokio::spawn(async move {
        let mut failures = 0;
        for round in 0..20u64 {
            for k in &writer_keys {
                let r = wc
                    .add(AddRequest {
                        hll_id: k.clone(),
                        item_hashes: items(k, 2000 + round * 50..2000 + (round + 1) * 50),
                        ..Default::default()
                    })
                    .await;
                if r.is_err() {
                    failures += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        failures
    });

    let draining = cluster.nodes[2].take().unwrap();
    let probe = draining.node.clone();
    let drain = tokio::spawn(draining.drain_and_shutdown());
    // Readiness flips immediately.
    wait_until("readyz 503 while draining", Duration::from_secs(2), || async { !probe.is_ready() }).await;
    drain.await.unwrap();

    let failures = writer.await.unwrap();
    assert_eq!(failures, 0, "client RPCs failed during drain");
    cluster.wait_active(2).await;

    // Allow async replication / repair to settle, then check exactness.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut c1 = cluster.client(1).await;
    for k in &keys {
        let expected = reference(&items(k, 0..3000)).estimate();
        let r = count(&mut c1, k).await;
        assert_eq!(r.cardinality, expected, "{k} after drain");
    }
    cluster.shutdown().await;
}

/// Scale 2 -> 3: the new node receives exactly the keys it now owns, old
/// owners GC what they no longer own, and counts are unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_up_rebalances_and_gcs() {
    let mut cluster = Cluster::start(2).await;
    let keys: Vec<String> = (0..150).map(|i| format!("scale-{i}")).collect();
    let mut c0 = cluster.client(0).await;
    for k in &keys {
        add(&mut c0, k, items(k, 0..800)).await;
    }
    cluster.add_node(2).await;
    cluster.wait_active(3).await;

    wait_until("holders == owners after scale-up", Duration::from_secs(20), || async {
        keys.iter().all(|k| sorted(cluster.holders(k)) == sorted(cluster.owner_names(k)))
    })
    .await;
    let on_new = keys.iter().filter(|k| cluster.node(2).node.store.contains(k)).count();
    assert!(on_new > 50, "new node holds only {on_new} keys");

    let mut c2 = cluster.client(2).await;
    for k in &keys {
        assert_eq!(count(&mut c2, k).await.cardinality, reference(&items(k, 0..800)).estimate());
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_into_cross_pod_union() {
    let cluster = Cluster::start(3).await;
    let mut c = cluster.client(1).await;
    let srcs: Vec<String> = (0..5).map(|i| format!("src-{i}")).collect();
    let mut all = Vec::new();
    for (i, s) in srcs.iter().enumerate() {
        let hs = items("shared", (i as u64 * 1000)..(i as u64 * 1000 + 3000));
        all.extend_from_slice(&hs);
        add(&mut c, s, hs).await;
    }
    let pre = items("dest", 0..500);
    all.extend_from_slice(&pre);
    add(&mut c, "dest", pre).await;

    let r = c
        .merge_into(MergeIntoRequest {
            dest_hll_id: "dest".into(),
            source_hll_ids: srcs.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let expected = reference(&all).estimate();
    assert_eq!(r.cardinality, expected);
    let mut c0 = cluster.client(0).await;
    assert_eq!(count(&mut c0, "dest").await.cardinality, expected);
    wait_until("dest replicated", Duration::from_secs(5), || async {
        sorted(cluster.holders("dest")) == sorted(cluster.owner_names("dest"))
    })
    .await;
    cluster.shutdown().await;
}

/// A forwarded write (hop_count=1) that lands on a non-owner is applied
/// locally (no second hop) and handed to the real primary asynchronously.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hop_limit_loop_fallback() {
    let cluster = Cluster::start(3).await;
    let id = "loop-key";
    let owners = cluster.owner_names(id);
    let outsider = (0..3)
        .find(|i| !owners.contains(&cluster.node(*i).node.cfg.node_id))
        .expect("R=2 of 3 leaves one non-owner");
    let mut c = cluster.client(outsider).await;
    c.add(AddRequest {
        hll_id: id.into(),
        item_hashes: items(id, 0..1234),
        hop_count: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let n = &cluster.node(outsider).node;
    assert_eq!(n.metrics.ops.with_label_values(&["add", "loop_fallback"]).get(), 1);

    let expected = reference(&items(id, 0..1234)).estimate();
    let primary = (0..3).find(|i| cluster.node(*i).node.cfg.node_id == owners[0]).unwrap();
    let mut cp = cluster.client(primary).await;
    wait_until("primary received fallback write", Duration::from_secs(5), || {
        let mut cp = cp.clone();
        async move { count(&mut cp, id).await.cardinality == expected }
    })
    .await;
    // And the outsider eventually GCs its stray copy.
    wait_until("outsider GC", Duration::from_secs(10), || async { !n.store.contains(id) }).await;
    let _ = count(&mut cp, id).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_endpoints_and_metrics() {
    let cluster = Cluster::start(1).await;
    let h = cluster.node(0);
    wait_until("liveness", Duration::from_secs(3), || async { h.node.is_live() }).await;
    assert!(h.node.is_ready());
    let body = http_get(h.metrics_addr, "/metrics").await;
    assert!(body.contains("hll_cluster_live_nodes 1"), "{body}");
    assert!(http_get(h.metrics_addr, "/readyz").await.starts_with("HTTP/1.1 200"));
    assert!(http_get(h.metrics_addr, "/healthz").await.starts_with("HTTP/1.1 200"));
    let _ = Arc::clone(&h.node);
    cluster.shutdown().await;
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

/// Spec chaos test #2 (rollout restart): drain hll-node-2, then recreate it
/// with the same name but a new address, new generation and empty RAM, all
/// under continuous write load. Includes a writer pinned to the draining pod
/// (clients keep HTTP/2 connections to it during `drain_wait`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_restart_same_name_rejoin() {
    let mut cluster = Cluster::start(3).await;
    let keys: Vec<String> = (0..60).map(|i| format!("roll-{i}")).collect();
    let mut c0 = cluster.client(0).await;
    for k in &keys {
        add(&mut c0, k, items(k, 0..1000)).await;
    }

    let writer_keys = keys.clone();
    let mut wc = cluster.client(0).await;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let writer = tokio::spawn(async move {
        let (mut failures, mut rounds) = (0u32, 0u64);
        while !*stop_rx.borrow() {
            for k in &writer_keys {
                let r = wc
                    .add(AddRequest {
                        hll_id: k.clone(),
                        item_hashes: items(k, 10_000 + rounds * 20..10_000 + (rounds + 1) * 20),
                        ..Default::default()
                    })
                    .await;
                failures += r.is_err() as u32;
            }
            rounds += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (failures, rounds)
    });

    let mut pinned = cluster.client(2).await;
    let draining = cluster.nodes[2].take().unwrap();
    let drain = tokio::spawn(draining.drain_and_shutdown());
    // Writes to the draining pod during drain_wait must be forwarded, not lost.
    let mut pinned_failures = 0;
    for k in &keys {
        let r = pinned
            .add(AddRequest {
                hll_id: k.clone(),
                item_hashes: items(k, 5_000..5_100),
                ..Default::default()
            })
            .await;
        pinned_failures += r.is_err() as u32;
    }
    assert_eq!(pinned_failures, 0, "writes to draining pod failed");
    drain.await.unwrap();

    // Recreate hll-node-2 (same POD_NAME, new IP/ports, new generation).
    cluster.add_node(2).await;
    cluster.wait_active(3).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop_tx.send(true).unwrap();
    let (failures, rounds) = writer.await.unwrap();
    assert_eq!(failures, 0, "client RPCs failed during rolling restart");
    assert!(rounds > 0);

    let expected: HashMap<String, u64> = keys
        .iter()
        .map(|k| {
            let mut hs = items(k, 0..1000);
            hs.extend(items(k, 5_000..5_100));
            hs.extend(items(k, 10_000..10_000 + rounds * 20));
            (k.clone(), reference(&hs).estimate())
        })
        .collect();

    wait_until("holders == owners after rejoin", Duration::from_secs(20), || async {
        keys.iter().all(|k| sorted(cluster.holders(k)) == sorted(cluster.owner_names(k)))
    })
    .await;
    let rejoined_owns = keys
        .iter()
        .filter(|k| cluster.owner_names(k).contains(&"hll-node-2".to_string()))
        .count();
    assert!(rejoined_owns > 0);
    for i in 0..3 {
        let c = cluster.client(i).await;
        wait_until(&format!("exact counts via node {i}"), Duration::from_secs(10), || {
            let c = c.clone();
            let keys = keys.clone();
            let expected = expected.clone();
            async move {
                let mut c = c;
                for k in &keys {
                    if count(&mut c, k).await.cardinality != expected[k] {
                        return false;
                    }
                }
                true
            }
        })
        .await;
    }
    cluster.shutdown().await;
}

/// REPLICATION_MODE=sync: both owners hold the write when Add returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_replication_mode() {
    let mut cluster = Cluster { nodes: Vec::new(), seed_port: 0 };
    for i in 0..3 {
        let seed = (i > 0).then_some(cluster.seed_port);
        let (mut cfg, gossip) = test_config(&format!("hll-node-{i}"), seed);
        cfg.replication_mode = hll_node::config::ReplicationMode::Sync;
        if i == 0 {
            cluster.seed_port = gossip;
        }
        cluster.nodes.push(Some(hll_node::start(cfg).await.unwrap()));
    }
    cluster.wait_active(3).await;
    let mut c = cluster.client(0).await;
    for i in 0..30 {
        let id = format!("sync-{i}");
        add(&mut c, &id, items(&id, 0..2000)).await;
        // No waiting: the replica must already have it.
        assert_eq!(sorted(cluster.holders(&id)), sorted(cluster.owner_names(&id)), "{id}");
    }
    cluster.shutdown().await;
}
