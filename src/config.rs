//! Configuration from environment variables (12-factor; see deploy/ manifests).

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Fire-and-forget onto per-peer batched channels (lowest latency).
    Async,
    /// Await replica acks before acknowledging the client.
    Sync,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Stable node identity (`POD_NAME`).
    pub node_id: String,
    /// Cluster isolation key (`CLUSTER_ID`).
    pub cluster_id: String,
    /// IP peers use to reach this node (`POD_IP`).
    pub advertise_ip: IpAddr,
    /// Bind IP for all listeners (`BIND_IP`, default 0.0.0.0).
    pub bind_ip: IpAddr,
    pub gossip_port: u16,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub seed_nodes: Vec<String>,
    pub az: Option<String>,

    pub replication_factor: usize,
    pub replication_mode: ReplicationMode,
    pub az_aware: bool,
    pub precision: u8,
    pub sparse_threshold_bytes: usize,
    /// Soft cap on estimated HLL RAM; 0 = unlimited.
    pub max_memory_bytes: u64,
    /// Serve Count from any owner (primary or replica) instead of the primary.
    pub local_reads: bool,

    pub gossip_interval: Duration,
    pub phi_threshold: f64,
    /// Failure-detector bootstrap interval (keep close to the gossip interval).
    pub fd_initial_interval: Duration,
    pub dead_node_grace_period: Duration,
    /// Minimum live nodes for /readyz.
    pub min_quorum: usize,

    pub rpc_timeout: Duration,
    pub replication_batch_interval: Duration,
    pub replication_queue_capacity: usize,
    pub drain_wait: Duration,
    pub orphan_grace: Duration,
    pub reconcile_interval: Duration,
    pub dirty_flush_interval: Duration,
    pub sweep_interval: Duration,
    pub tombstone_ttl: Duration,
    pub handoff_concurrency: usize,
    pub max_message_bytes: usize,
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

fn env_parse<T: FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_opt(key) {
        None => Ok(default),
        Some(v) => v
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid {key}={v:?}: {e}")),
    }
}

fn env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match env_opt(key).map(|v| v.to_ascii_lowercase()) {
        None => Ok(default),
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => Ok(true),
        Some(v) if matches!(v.as_str(), "0" | "false" | "no" | "off") => Ok(false),
        Some(v) => bail!("invalid {key}={v:?}: expected true/false"),
    }
}

fn env_ms(key: &str, default_ms: u64) -> anyhow::Result<Duration> {
    Ok(Duration::from_millis(env_parse(key, default_ms)?))
}

impl Config {
    /// Defaults suitable for a single-node local run; override via env.
    pub fn local(node_id: &str, gossip_port: u16, grpc_port: u16, metrics_port: u16) -> Self {
        Self {
            node_id: node_id.to_owned(),
            cluster_id: "eks-hll-dev".into(),
            advertise_ip: IpAddr::from([127, 0, 0, 1]),
            bind_ip: IpAddr::from([127, 0, 0, 1]),
            gossip_port,
            grpc_port,
            metrics_port,
            seed_nodes: Vec::new(),
            az: None,
            replication_factor: 2,
            replication_mode: ReplicationMode::Async,
            az_aware: false,
            precision: 14,
            sparse_threshold_bytes: crate::hll::DEFAULT_SPARSE_THRESHOLD_BYTES,
            max_memory_bytes: 0,
            local_reads: false,
            gossip_interval: Duration::from_millis(500),
            phi_threshold: 8.0,
            fd_initial_interval: Duration::from_millis(500),
            dead_node_grace_period: Duration::from_secs(3600),
            min_quorum: 1,
            rpc_timeout: Duration::from_secs(2),
            replication_batch_interval: Duration::from_millis(20),
            replication_queue_capacity: 65_536,
            drain_wait: Duration::from_secs(2),
            orphan_grace: Duration::from_secs(30),
            reconcile_interval: Duration::from_secs(30),
            dirty_flush_interval: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(30),
            tombstone_ttl: Duration::from_secs(60),
            handoff_concurrency: 64,
            max_message_bytes: 32 * 1024 * 1024,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let node_id = env_opt("POD_NAME")
            .or_else(|| env_opt("NODE_ID"))
            .or_else(|| env_opt("HOSTNAME"))
            .context("POD_NAME (or NODE_ID) must be set")?;
        let gossip_port = env_parse("GOSSIP_PORT", 7280u16)?;
        let grpc_port = env_parse("GRPC_PORT", 8080u16)?;
        let metrics_port = env_parse("METRICS_PORT", 9090u16)?;
        let mut c = Self::local(&node_id, gossip_port, grpc_port, metrics_port);

        c.cluster_id = env_opt("CLUSTER_ID").unwrap_or(c.cluster_id);
        c.advertise_ip = env_parse("POD_IP", c.advertise_ip)?;
        c.bind_ip = env_parse("BIND_IP", IpAddr::from([0, 0, 0, 0]))?;
        c.seed_nodes = env_opt("SEED_NODES")
            .map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_owned())
                    .filter(|x| !x.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        c.az = match env_opt("AZ") {
            Some(az) => Some(az),
            None => env_opt("AZ_FILE")
                .and_then(|p| std::fs::read_to_string(p).ok())
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty()),
        };
        c.replication_factor = env_parse("REPLICATION_FACTOR", c.replication_factor)?;
        c.replication_mode = match env_opt("REPLICATION_MODE")
            .unwrap_or_else(|| "async".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "async" => ReplicationMode::Async,
            "sync" => ReplicationMode::Sync,
            other => bail!("invalid REPLICATION_MODE={other:?}: expected async|sync"),
        };
        c.az_aware = env_bool("AZ_AWARE", c.az.is_some())?;
        c.precision = env_parse("HLL_PRECISION", c.precision)?;
        c.sparse_threshold_bytes = env_parse("SPARSE_THRESHOLD_BYTES", c.sparse_threshold_bytes)?;
        c.max_memory_bytes = env_parse("MAX_MEMORY_BYTES", c.max_memory_bytes)?;
        c.local_reads = env_bool("LOCAL_READS", c.local_reads)?;
        c.gossip_interval = env_ms("GOSSIP_INTERVAL_MS", 500)?;
        c.phi_threshold = env_parse("PHI_THRESHOLD", c.phi_threshold)?;
        c.fd_initial_interval = env_ms("FD_INITIAL_INTERVAL_MS", c.gossip_interval.as_millis() as u64)?;
        c.dead_node_grace_period = Duration::from_secs(env_parse("DEAD_NODE_GRACE_SECS", 3600u64)?);
        c.min_quorum = env_parse("MIN_QUORUM", c.min_quorum)?;
        c.rpc_timeout = env_ms("RPC_TIMEOUT_MS", 2000)?;
        c.replication_batch_interval = env_ms("REPLICATION_BATCH_MS", 20)?;
        c.replication_queue_capacity = env_parse("REPLICATION_QUEUE_CAPACITY", c.replication_queue_capacity)?;
        c.drain_wait = env_ms("DRAIN_WAIT_MS", 2000)?;
        c.orphan_grace = Duration::from_secs(env_parse("ORPHAN_GRACE_SECS", 30u64)?);
        c.reconcile_interval = Duration::from_secs(env_parse("RECONCILE_INTERVAL_SECS", 30u64)?);
        c.dirty_flush_interval = env_ms("DIRTY_FLUSH_MS", 2000)?;
        c.sweep_interval = Duration::from_secs(env_parse("SWEEP_INTERVAL_SECS", 30u64)?);
        c.tombstone_ttl = Duration::from_secs(env_parse("TOMBSTONE_TTL_SECS", 60u64)?);
        c.handoff_concurrency = env_parse("HANDOFF_CONCURRENCY", c.handoff_concurrency)?;
        c.validate()?;
        Ok(c)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !(crate::hll::MIN_PRECISION..=crate::hll::MAX_PRECISION).contains(&self.precision) {
            bail!("HLL_PRECISION must be in {}..={}", crate::hll::MIN_PRECISION, crate::hll::MAX_PRECISION);
        }
        if self.replication_factor == 0 {
            bail!("REPLICATION_FACTOR must be >= 1");
        }
        if self.advertise_ip.is_unspecified() {
            bail!("POD_IP must be a routable address, not {}", self.advertise_ip);
        }
        Ok(())
    }

    pub fn gossip_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.gossip_port)
    }
    pub fn gossip_advertise_addr(&self) -> SocketAddr {
        SocketAddr::new(self.advertise_ip, self.gossip_port)
    }
    pub fn grpc_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.grpc_port)
    }
    pub fn rpc_advertise_addr(&self) -> String {
        SocketAddr::new(self.advertise_ip, self.grpc_port).to_string()
    }
    pub fn metrics_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.metrics_port)
    }
}
