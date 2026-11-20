//! Lock-free routing plane: an `ArcSwap` snapshot of active peers plus
//! Rendezvous (Highest-Random-Weight) hashing with xxh3_64.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use xxhash_rust::xxh3::Xxh3;

pub const STATE_ACTIVE: &str = "active";
pub const STATE_DRAINING: &str = "draining";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub node_id: Arc<str>,
    pub generation_id: u64,
    pub rpc_addr: String,
    pub az: Option<String>,
}

/// One live node as reported by gossip (before filtering).
#[derive(Debug, Clone)]
pub struct LiveNode {
    pub node_id: String,
    pub generation_id: u64,
    pub rpc_addr: Option<String>,
    pub state: Option<String>,
    pub az: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Membership {
    /// Active peers (state == "active", rpc_addr published), sorted by node_id.
    pub active: Vec<Arc<Peer>>,
    /// Number of live nodes (any state) after generation de-duplication.
    pub live_count: usize,
}

impl Membership {
    /// Build from gossip: dedupe by node_id keeping the highest generation,
    /// keep only `active` nodes that have published an `rpc_addr`.
    pub fn from_live_nodes(nodes: impl IntoIterator<Item = LiveNode>) -> Self {
        let mut latest: BTreeMap<String, LiveNode> = BTreeMap::new();
        for n in nodes {
            match latest.get(&n.node_id) {
                Some(prev) if prev.generation_id >= n.generation_id => {}
                _ => {
                    latest.insert(n.node_id.clone(), n);
                }
            }
        }
        let live_count = latest.len();
        let active = latest
            .into_values()
            .filter(|n| n.state.as_deref() == Some(STATE_ACTIVE))
            .filter_map(|n| {
                Some(Arc::new(Peer {
                    node_id: n.node_id.into(),
                    generation_id: n.generation_id,
                    rpc_addr: n.rpc_addr?,
                    az: n.az.filter(|s| !s.is_empty()),
                }))
            })
            .collect();
        Self { active, live_count }
    }

    pub fn contains(&self, node_id: &str) -> bool {
        self.active.iter().any(|p| &*p.node_id == node_id)
    }

    /// Same routing-relevant view (ids, incarnations, addresses).
    pub fn same_routing(&self, other: &Membership) -> bool {
        self.active.len() == other.active.len()
            && self
                .active
                .iter()
                .zip(&other.active)
                .all(|(a, b)| a.same_instance(b) && a.rpc_addr == b.rpc_addr)
    }
}

#[inline]
pub fn score(hll_id: &str, node_id: &str) -> u64 {
    let mut h = Xxh3::new();
    h.update(hll_id.as_bytes());
    h.update(&[0x00]);
    h.update(node_id.as_bytes());
    h.digest()
}

/// Top-`k` owners of `hll_id` in `peers`, ordered (primary first).
///
/// With `az_aware`, owners after the primary prefer the highest-scoring peer
/// in an AZ not yet represented; if every AZ is already used, plain score
/// order is used for the remainder.
pub fn top_k(peers: &[Arc<Peer>], hll_id: &str, k: usize, az_aware: bool) -> Vec<Arc<Peer>> {
    if peers.is_empty() || k == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(u64, &Arc<Peer>)> =
        peers.iter().map(|p| (score(hll_id, &p.node_id), p)).collect();
    // Descending score; node_id breaks (astronomically unlikely) ties.
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.node_id.cmp(&b.1.node_id)));
    let k = k.min(scored.len());
    if !az_aware {
        return scored.into_iter().take(k).map(|(_, p)| p.clone()).collect();
    }
    let mut chosen: Vec<Arc<Peer>> = Vec::with_capacity(k);
    let mut taken = vec![false; scored.len()];
    while chosen.len() < k {
        let pick = scored
            .iter()
            .enumerate()
            .find(|(i, (_, p))| {
                !taken[*i]
                    && match &p.az {
                        Some(az) => !chosen.iter().any(|c| c.az.as_ref() == Some(az)),
                        None => true,
                    }
            })
            .or_else(|| scored.iter().enumerate().find(|(i, _)| !taken[*i]))
            .map(|(i, _)| i)
            .expect("k <= len");
        taken[pick] = true;
        chosen.push(scored[pick].1.clone());
    }
    chosen
}

pub struct Router {
    self_id: Arc<str>,
    replication_factor: usize,
    az_aware: bool,
    membership: ArcSwap<Membership>,
    self_draining: AtomicBool,
}

impl Router {
    pub fn new(self_id: impl Into<Arc<str>>, replication_factor: usize, az_aware: bool) -> Self {
        Self {
            self_id: self_id.into(),
            replication_factor: replication_factor.max(1),
            az_aware,
            membership: ArcSwap::from_pointee(Membership::default()),
            self_draining: AtomicBool::new(false),
        }
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    pub fn update(&self, m: Membership) {
        self.membership.store(Arc::new(m));
    }

    pub fn membership(&self) -> Arc<Membership> {
        self.membership.load_full()
    }

    /// Once draining, this node never routes to itself, even before its own
    /// state change has round-tripped through gossip.
    pub fn set_self_draining(&self, draining: bool) {
        self.self_draining.store(draining, Ordering::SeqCst);
    }

    pub fn is_self_draining(&self) -> bool {
        self.self_draining.load(Ordering::SeqCst)
    }

    /// Owners of `hll_id` (primary first) under a given membership.
    pub fn owners_in(&self, m: &Membership, hll_id: &str) -> Vec<Arc<Peer>> {
        if self.is_self_draining() {
            let peers: Vec<Arc<Peer>> = m
                .active
                .iter()
                .filter(|p| *p.node_id != *self.self_id)
                .cloned()
                .collect();
            top_k(&peers, hll_id, self.replication_factor, self.az_aware)
        } else {
            top_k(&m.active, hll_id, self.replication_factor, self.az_aware)
        }
    }

    pub fn top_k_owners(&self, hll_id: &str) -> Vec<Arc<Peer>> {
        let m = self.membership.load();
        self.owners_in(&m, hll_id)
    }

    pub fn is_self(&self, p: &Peer) -> bool {
        *p.node_id == *self.self_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn peers(n: usize) -> Vec<Arc<Peer>> {
        (0..n)
            .map(|i| {
                Arc::new(Peer {
                    node_id: format!("hll-node-{i}").into(),
                    generation_id: 1,
                    rpc_addr: format!("10.0.0.{i}:8080"),
                    az: Some(format!("az-{}", i % 3)),
                })
            })
            .collect()
    }

    #[test]
    fn deterministic_and_distinct() {
        let ps = peers(5);
        let a = top_k(&ps, "key-1", 2, false);
        let b = top_k(&ps, "key-1", 2, false);
        assert_eq!(a, b);
        assert_ne!(a[0].node_id, a[1].node_id);
        // Order of input does not matter.
        let mut rev = ps.clone();
        rev.reverse();
        assert_eq!(top_k(&rev, "key-1", 2, false), a);
    }

    #[test]
    fn known_score_is_stable() {
        // Pins the score function so a dependency bump can't silently reshuffle.
        let mut buf = b"key".to_vec();
        buf.push(0);
        buf.extend_from_slice(b"node");
        assert_eq!(score("key", "node"), xxhash_rust::xxh3::xxh3_64(&buf));
    }

    #[test]
    fn balanced_distribution() {
        let ps = peers(5);
        let mut counts: HashMap<Arc<str>, usize> = HashMap::new();
        let n = 50_000;
        for i in 0..n {
            let o = top_k(&ps, &format!("hll-{i}"), 1, false);
            *counts.entry(o[0].node_id.clone()).or_default() += 1;
        }
        let ideal = n as f64 / 5.0;
        for (_, c) in counts {
            assert!(((c as f64 - ideal) / ideal).abs() < 0.05, "{c}");
        }
    }

    #[test]
    fn minimal_disruption_on_join() {
        let before = peers(4);
        let after = peers(5);
        let n = 20_000;
        let mut moved = 0;
        for i in 0..n {
            let k = format!("hll-{i}");
            let a = top_k(&before, &k, 1, false)[0].node_id.clone();
            let b = top_k(&after, &k, 1, false)[0].node_id.clone();
            if a != b {
                // Keys only ever move *to* the new node.
                assert_eq!(&*b, "hll-node-4");
                moved += 1;
            }
        }
        let frac = moved as f64 / n as f64;
        assert!((frac - 0.2).abs() < 0.02, "moved {frac}");
    }

    #[test]
    fn replica_promoted_on_primary_loss() {
        let ps = peers(5);
        for i in 0..1000 {
            let k = format!("hll-{i}");
            let owners = top_k(&ps, &k, 2, false);
            let survivors: Vec<_> = ps
                .iter()
                .filter(|p| p.node_id != owners[0].node_id)
                .cloned()
                .collect();
            assert_eq!(top_k(&survivors, &k, 1, false)[0], owners[1]);
        }
    }

    #[test]
    fn az_aware_spreads_replicas() {
        let ps = peers(6);
        for i in 0..1000 {
            let o = top_k(&ps, &format!("k{i}"), 2, true);
            assert_ne!(o[0].az, o[1].az);
            // Primary is unchanged by AZ awareness.
            assert_eq!(o[0], top_k(&ps, &format!("k{i}"), 1, false)[0]);
        }
    }

    #[test]
    fn membership_dedupes_generations_and_filters_state() {
        let m = Membership::from_live_nodes(vec![
            LiveNode {
                node_id: "a".into(),
                generation_id: 1,
                rpc_addr: Some("old:1".into()),
                state: Some("active".into()),
                az: None,
            },
            LiveNode {
                node_id: "a".into(),
                generation_id: 2,
                rpc_addr: Some("new:1".into()),
                state: Some("active".into()),
                az: None,
            },
            LiveNode {
                node_id: "b".into(),
                generation_id: 1,
                rpc_addr: Some("b:1".into()),
                state: Some("draining".into()),
                az: None,
            },
            LiveNode {
                node_id: "c".into(),
                generation_id: 1,
                rpc_addr: None,
                state: Some("active".into()),
                az: None,
            },
        ]);
        assert_eq!(m.live_count, 3);
        assert_eq!(m.active.len(), 1);
        assert_eq!(m.active[0].rpc_addr, "new:1");
    }

    #[test]
    fn draining_self_excluded() {
        let r = Router::new("hll-node-0", 2, false);
        r.update(Membership {
            active: peers(3),
            live_count: 3,
        });
        r.set_self_draining(true);
        for i in 0..200 {
            assert!(
                r.top_k_owners(&format!("k{i}"))
                    .iter()
                    .all(|p| &*p.node_id != "hll-node-0")
            );
        }
    }
}
