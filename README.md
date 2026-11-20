<div align="center">

# ⚡ hll-node

### Count billions of unique things across a cluster with no database, no Redis and no ZooKeeper

**A distributed HyperLogLog engine written in Rust.** Pods gossip with each other, place keys with rendezvous hashing and merge state as CRDTs.<br/>
It is a single binary, keeps all its state in RAM, and recovers from failures without any consensus protocol.

[![CI](https://github.com/gregoirelinot/hll-node/actions/workflows/ci.yml/badge.svg)](https://github.com/gregoirelinot/hll-node/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange?logo=rust)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![gRPC](https://img.shields.io/badge/API-gRPC-244c5a?logo=grpc)](proto/hll.proto)
[![Kubernetes](https://img.shields.io/badge/deploy-EKS%20%7C%20k8s-326ce5?logo=kubernetes&logoColor=white)](deploy/hll-node.yaml)
[![Docker](https://img.shields.io/badge/image-47MB_distroless-2496ed?logo=docker&logoColor=white)](Dockerfile)

```
 245M inserts/sec/core   ·   0.95 µs 16 KiB merge   ·   ±0.81% std error   ·   ≤1 network hop
```

[Why](#-why) · [Quickstart](#-quickstart) · [Architecture](#-architecture) · [Benchmarks](#-benchmarks) · [Chaos-tested](#-chaos-tested) · [Deploy](#-deploy-on-eks) · [Config](#%EF%B8%8F-configuration)

</div>

---

## 🤔 Why

You want to know **how many unique users, IPs, sessions or events** you have seen, and you have billions of them.
Exact counting would need terabytes of RAM. A HyperLogLog does it in **16 KiB per counter**, within **±0.81%**.

Running HLLs *at scale* is usually where it gets painful: a Redis cluster to babysit, a coordinator, a consensus layer and failover scripts.

**hll-node avoids all of that.** HLL merge is just element-wise `max`, which is commutative, associative and idempotent. That makes it a perfect CRDT. So:

- 🔁 **Replication never needs consensus.** Merging any copies in any order gives the same result.
- 💥 **A crashed pod loses nothing.** Its replica already holds a copy in RAM, and the replica becomes primary within about 2–3 seconds.
- 🧘 **Split-brain cannot corrupt state.** Diverged replicas converge deterministically.

## ✨ Features

|  |  |
|---|---|
| 🕸️ **Zero external state** | Pods form a P2P mesh over the pod network (Scuttlebutt gossip via [`chitchat`](https://github.com/quickwit-oss/chitchat)) |
| 🎯 **Deterministic placement** | Rendezvous hashing with `xxh3_64`. Adding a pod moves only `1/(N+1)` of keys |
| 🚀 **Any pod serves any request** | A request for a non-local key is forwarded in **at most 1 hop**, and hop counting prevents loops |
| 🧬 **Sparse → dense** | Small HLLs use a few hundred bytes and switch to 16 KiB dense automatically |
| 📐 **Unbiased estimator** | [Ertl's improved estimator](https://arxiv.org/abs/1702.01284): no bias tables, accurate from 1 to billions |
| 🌍 **AZ-aware replicas** | Replicas are placed in a different AWS AZ, so a full AZ outage loses no data |
| 🔄 **Graceful drain** | On `SIGTERM` the pod gossips `draining`, streams its state to the new owners and exits 0 |
| ⚡ **SIMD merges** | The `max` loop auto-vectorizes to `vpmaxub` on x86 and `umax` on ARM |
| 🗜️ **Compact wire format** | zstd-compressed snapshots: 16 KiB becomes about 6.7 KiB |
| ⏳ **TTL + memory cap** | Background TTL eviction. New keys get `RESOURCE_EXHAUSTED` instead of the pod being OOM-killed |
| 🪦 **Deletes that stay deleted** | Tombstones stop CRDT merges from bringing deleted keys back |
| 📊 **Observable** | Prometheus metrics, `/healthz` and `/readyz` |

## 🚀 Quickstart

**Run a 2-node cluster on your laptop:**

```bash
git clone https://github.com/gregoirelinot/hll-node && cd hll-node

# node 0
POD_NAME=n0 POD_IP=127.0.0.1 SEED_NODES=127.0.0.1:7280 cargo run --release

# node 1 (another terminal)
POD_NAME=n1 POD_IP=127.0.0.1 GOSSIP_PORT=7281 GRPC_PORT=8081 METRICS_PORT=9091 \
  SEED_NODES=127.0.0.1:7280 cargo run --release
```

**Or with Docker:**

```bash
docker build -t hll-node .
docker network create --subnet 172.30.0.0/24 hllnet
for i in 0 1; do
  docker run -d --name hll-$i --network hllnet --ip 172.30.0.1$i -p 1909$i:9090 -p 808$i:8080 \
    -e POD_NAME=hll-node-$i -e POD_IP=172.30.0.1$i -e SEED_NODES=172.30.0.10:7280 hll-node
done
curl localhost:19090/metrics | grep hll_cluster   # → hll_cluster_live_nodes 2
```

**Then talk gRPC** ([`proto/hll.proto`](proto/hll.proto)):

```bash
grpcurl -plaintext -import-path proto -proto hll.proto \
  -d '{"hll_id":"dau:2026-11-20","items":["YWxpY2U=","Ym9i"]}' localhost:8080 hll.v1.HllService/Add

grpcurl -plaintext -import-path proto -proto hll.proto \
  -d '{"hll_id":"dau:2026-11-20"}' localhost:8081 hll.v1.HllService/Count
# → { "cardinality": "2", "exists": true }   ← any node answers, ≤1 hop
```

| RPC | What it does |
|---|---|
| `Add` | Insert raw items or pre-hashed `u64`s, with an optional TTL |
| `Count` | Cardinality estimate |
| `MergeInto` | Union of N HLLs that may live on different pods |
| `Delete` | Delete the key on every replica, with a tombstone |

## 🏗️ Architecture

```text
                  Client Traffic (ALB / NLB / ClusterIP Service)
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          ▼                            ▼                            ▼
   ┌─────────────┐              ┌─────────────┐              ┌─────────────┐
   │  hll-node-0 │◄── UDP 7280 ─►│  hll-node-1 │◄── UDP 7280 ─►│  hll-node-2 │
   │   (AZ-a)    │   chitchat   │   (AZ-b)    │   chitchat   │   (AZ-c)    │
   │  gRPC 8080  │◄────────────►│  gRPC 8080  │◄────────────►│  gRPC 8080  │
   └─────────────┘   data plane └─────────────┘              └─────────────┘
```

There are three layers in one binary:

| Plane | Tech | Job |
|---|---|---|
| **Control** | `chitchat` (UDP), phi-accrual failure detection | Who is alive. Gossips only `rpc_addr`, `state` and `az`, never HLL data |
| **Routing** | `ArcSwap` snapshot plus `xxh3` HRW | `owners(id) = top-R peers by xxh3(id ‖ 0x00 ‖ node_id)`, computed lock-free on every request |
| **Data** | tonic gRPC / HTTP/2 | Client API, 1-hop forwarding, batched async replication, snapshot handoff |

**Write path:** client → any pod → (≤1 hop) → primary → local `DashMap` → batched `ReplicateDelta` to replicas every 10–50 ms.<br/>
**Failure path:** a replica is already in RAM, so it is promoted on the next routing snapshot. A background reconciler then restores `R` copies.

📁 Code map: [`hll.rs`](src/hll.rs) sketch · [`store.rs`](src/store.rs) storage · [`router.rs`](src/router.rs) HRW · [`cluster.rs`](src/cluster.rs) gossip · [`node.rs`](src/node.rs) request flows, replication, rebalancing · [`health.rs`](src/health.rs) probes and metrics

## 📈 Benchmarks

`cargo run --release --example bench` on one core of an AMD Ryzen 5 5500, precision 14:

| Operation | Result |
|---|---|
| Insert (pre-hashed) | **245 M items/s** |
| Dense merge (16 KiB) | **0.95 µs** |
| Estimate | **8.5 µs** |
| 10M items, estimated | 9,938,536 (**−0.62%**) |
| Snapshot over the wire | 16,384 → **6,778 bytes** (zstd) |
| Docker image | **47 MB** (distroless, non-root) |

These are micro-benchmarks of the sketch engine. End-to-end RPC throughput depends on your network and has not been benchmarked.

**Capacity:** at 16 KiB per dense HLL with R=2, **100k HLLs ≈ 3.2 GiB** of RAM across the cluster. Sparse HLLs are much smaller.

## 💥 Chaos-tested

Every scenario below runs **real gossip over UDP and real gRPC over TCP** between in-process nodes ([`tests/cluster.rs`](tests/cluster.rs)):

| Scenario | Asserted |
|---|---|
| ☠️ **kill -9 a pod** | Every count is **bit-identical** to its pre-crash value, and R=2 is restored on the survivors |
| 🔄 **Rolling restart** | A pod drains and rejoins under the same name with empty RAM, under write load. **0 failed RPCs** and exact counts everywhere |
| 🛬 **Drain under load** | **0 failed RPCs**, and `/readyz` returns 503 immediately |
| 📈 **Scale 2 → 3** | The new pod receives exactly the keys it now owns, and old owners GC their copies |
| 🔀 **MergeInto** | The result equals a single-sketch union exactly |
| 🪦 **Delete** | Never resurrected by replication, repair or rebalancing |
| 🔁 **Hop limit** | A stale forwarded write is applied locally and handed to the primary, with no second hop |
| 🔒 **Sync replication** | Both owners hold the write before `Add` returns |

Unit tests cover estimator accuracy from 10 to 1M items, unbiasedness, the CRDT laws, corrupt wire payloads, HRW balance within ±5% and minimal key movement.

```bash
cargo test   # 35 tests, ~5s
```

## ☸️ Deploy on EKS

```bash
docker buildx build --platform linux/arm64 -t <acct>.dkr.ecr.<region>.amazonaws.com/hll-node:v1.0.0 --push .
kubectl apply -f deploy/hll-node.yaml
kubectl -n hll-system get pods -w
```

[`deploy/hll-node.yaml`](deploy/hll-node.yaml) contains:
- A **StatefulSet** whose stable pod names give stable hashing, spread across zones and hosts.
- A **headless Service** used for seed discovery, plus a **ClusterIP** Service for clients.
- A **PDB** with `maxUnavailable: 1`, which protects R=2.
- An **init container** that reads the node's AZ label for AZ-aware replicas, with minimal RBAC.
- A **NetworkPolicy**, a Guaranteed QoS class (4 GiB), and a read-only root filesystem.

## ⚙️ Configuration

All configuration is through environment variables:

| Var | Default | |
|---|---|---|
| `POD_NAME` | **required** | Stable node id, used for hashing |
| `POD_IP` | `127.0.0.1` | Advertised address |
| `SEED_NODES` | — | `host:7280,host:7280` |
| `REPLICATION_FACTOR` | `2` | |
| `REPLICATION_MODE` | `async` | `sync` waits for replica acks |
| `HLL_PRECISION` | `14` | 4–18, standard error 1.04/√2^p |
| `MAX_MEMORY_BYTES` | `0` (∞) | Soft cap for new keys |
| `AZ` / `AZ_FILE` | — | Turns on AZ-aware placement |
| `LOCAL_READS` | `false` | Serve `Count` from replicas too |
| `MIN_QUORUM` | `1` | Live nodes required for `/readyz` |
| `GOSSIP_INTERVAL_MS` · `PHI_THRESHOLD` | `500` · `8.0` | Failure-detection tuning |
| `DRAIN_WAIT_MS` · `ORPHAN_GRACE_SECS` · `TOMBSTONE_TTL_SECS` | `2000` · `30` · `60` | Lifecycle timing |

<details>
<summary>All knobs</summary>

`GRPC_PORT` `GOSSIP_PORT` `METRICS_PORT` `CLUSTER_ID` `BIND_IP` `RPC_TIMEOUT_MS` `REPLICATION_BATCH_MS` `REPLICATION_QUEUE_CAPACITY` `RECONCILE_INTERVAL_SECS` `DIRTY_FLUSH_MS` `SWEEP_INTERVAL_SECS` `HANDOFF_CONCURRENCY` `FD_INITIAL_INTERVAL_MS` `DEAD_NODE_GRACE_SECS` `SPARSE_THRESHOLD_BYTES` `AZ_AWARE` — see [`src/config.rs`](src/config.rs).
</details>

## 📊 Metrics

`hll_cluster_live_nodes` · `hll_cluster_active_nodes` · `hll_keys_total{mode}` · `hll_memory_bytes` · `hll_ops_total{op,route}` · `hll_replication_lag_ms` · `hll_replication_dropped_total` · `hll_snapshots_pushed_total{reason,result}` · `hll_dirty_keys` · `hll_ttl_evicted_total` · `hll_ownership_gc_total`

## 🧠 Design notes

<details>
<summary>The hard parts and the trade-offs</summary>

- **Consistency is AP with strong eventual consistency.** Writes are always accepted by reachable pods. If every owner is unreachable, the write is applied locally and repaired later.
- **Repair set.** A dropped or failed delta, or a fallback write, marks the key. A background flush then pushes a full snapshot to its owners.
- **Generation-aware rebalancing.** A pod that restarts with the same name has empty RAM. Its new `generation_id` (Unix milliseconds) makes it count as a new owner, so it receives snapshots.
- **Delete vs CRDT.** With max-merge alone, deleted keys would come back. Delete therefore fans out to every owner, and each owner keeps a tombstone that rejects internal writes for `TOMBSTONE_TTL_SECS`. A new client write clears it.
- **Memory cap.** It applies to the tracked HLL footprint, not cgroup RSS. Replication may go up to 10% over it, so durability doesn't fail first.
- **Known gap.** For roughly a second after a restart or scale-up, a pod that has just become owner can return a low count until its snapshot arrives. That is the eventual in eventual consistency.
- **Proto additions** to the original spec, all backward compatible: `GetSnapshot`, raw `items`, `Delete.internal`, snapshot `encoding`, `Count.allow_local`.
- **Not implemented:** 6-bit register packing. Registers are stored as `u8` for speed.
</details>

## 🛣️ Roadmap

- [ ] 6-bit packed dense registers (25% less RAM)
- [ ] Batched multi-key `ReplicateDelta`
- [ ] Count served from the merged copies of all owners right after rebalancing
- [ ] Helm chart
- [ ] End-to-end RPC load benchmark

## 🤝 Contributing

Contributions are welcome. Run `cargo test && cargo clippy --all-targets` before opening a PR.

## 📜 License

[MIT](LICENSE)

<div align="center">
<sub>Built with 🦀 Rust · chitchat · tonic · DashMap · xxh3 · zstd</sub><br/>
<sub>⭐ Star this repo if it saved you from running yet another Redis cluster.</sub>
</div>
