//! Prometheus metrics (served on the metrics port at /metrics).

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

pub struct Metrics {
    pub registry: Registry,
    pub live_nodes: IntGauge,
    pub active_nodes: IntGauge,
    pub keys: IntGaugeVec,
    pub memory_bytes: IntGauge,
    pub max_memory_bytes: IntGauge,
    pub ops: IntCounterVec,
    pub replication_lag_ms: Histogram,
    pub replication_dropped: IntCounter,
    pub replication_failed: IntCounter,
    pub snapshots_pushed: IntCounterVec,
    pub evicted: IntCounter,
    pub gc_dropped: IntCounter,
    pub dirty_keys: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        macro_rules! reg {
            ($m:expr) => {{
                let m = $m;
                registry.register(Box::new(m.clone())).expect("metric registration");
                m
            }};
        }
        Self {
            live_nodes: reg!(IntGauge::new("hll_cluster_live_nodes", "Live peers seen by chitchat (incl. self)").unwrap()),
            active_nodes: reg!(IntGauge::new("hll_cluster_active_nodes", "Peers in the active routing set").unwrap()),
            keys: reg!(IntGaugeVec::new(Opts::new("hll_keys_total", "HLLs held in RAM"), &["mode"]).unwrap()),
            memory_bytes: reg!(IntGauge::new("hll_memory_bytes", "Estimated HLL RAM usage").unwrap()),
            max_memory_bytes: reg!(IntGauge::new("hll_max_memory_bytes", "Configured MAX_MEMORY_BYTES").unwrap()),
            ops: reg!(IntCounterVec::new(Opts::new("hll_ops_total", "Operations by route"), &["op", "route"]).unwrap()),
            replication_lag_ms: reg!(Histogram::with_opts(
                HistogramOpts::new("hll_replication_lag_ms", "Enqueue-to-ack replication latency")
                    .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0])
            ).unwrap()),
            replication_dropped: reg!(IntCounter::new("hll_replication_dropped_total", "Deltas dropped because a peer queue was full").unwrap()),
            replication_failed: reg!(IntCounter::new("hll_replication_failed_total", "Delta RPCs that failed (key marked for snapshot repair)").unwrap()),
            snapshots_pushed: reg!(IntCounterVec::new(Opts::new("hll_snapshots_pushed_total", "PushSnapshot calls by reason/result"), &["reason", "result"]).unwrap()),
            evicted: reg!(IntCounter::new("hll_ttl_evicted_total", "HLLs evicted by TTL").unwrap()),
            gc_dropped: reg!(IntCounter::new("hll_ownership_gc_total", "Local HLLs dropped after ownership moved").unwrap()),
            dirty_keys: reg!(IntGauge::new("hll_dirty_keys", "Keys pending snapshot repair").unwrap()),
            registry,
        }
    }

    pub fn op(&self, op: &str, route: &str) {
        self.ops.with_label_values(&[op, route]).inc();
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("text encoding");
        String::from_utf8(buf).unwrap_or_default()
    }
}
