//! Control plane: chitchat membership (Scuttlebutt gossip + phi-accrual).
//! Gossips only node metadata: `rpc_addr`, `state`, `az`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, spawn_chitchat};
use tracing::{info, warn};

use crate::node::Node;
use crate::router::{LiveNode, Membership, STATE_ACTIVE, STATE_DRAINING};

pub const KEY_RPC_ADDR: &str = "rpc_addr";
pub const KEY_STATE: &str = "state";
pub const KEY_AZ: &str = "az";

pub fn generation_id() -> u64 {
    // Millisecond precision so that a pod restarted within the same second
    // still supersedes its previous incarnation.
    crate::node::now_unix_ms()
}

pub async fn start(node: &Arc<Node>) -> anyhow::Result<ChitchatHandle> {
    let cfg = &node.cfg;
    let chitchat_id = ChitchatId::new(cfg.node_id.clone(), generation_id(), cfg.gossip_advertise_addr());
    let config = ChitchatConfig {
        chitchat_id,
        cluster_id: cfg.cluster_id.clone(),
        extra_cluster_ids: HashSet::new(),
        gossip_interval: cfg.gossip_interval,
        listen_addr: cfg.gossip_listen_addr(),
        seed_nodes: cfg.seed_nodes.clone(),
        failure_detector_config: FailureDetectorConfig {
            phi_threshold: cfg.phi_threshold,
            sampling_window_size: 1_000,
            max_interval: std::time::Duration::from_secs(10),
            initial_interval: cfg.fd_initial_interval,
            dead_node_grace_period: cfg.dead_node_grace_period,
        },
        marked_for_deletion_grace_period: std::time::Duration::from_secs(3600),
        catchup_callback: None,
        extra_liveness_predicate: None,
        protocol_version: chitchat::ProtocolVersion::V0,
    };
    let mut kvs = vec![
        (KEY_RPC_ADDR.to_owned(), cfg.rpc_advertise_addr()),
        (KEY_STATE.to_owned(), STATE_ACTIVE.to_owned()),
    ];
    if let Some(az) = &cfg.az {
        kvs.push((KEY_AZ.to_owned(), az.clone()));
    }
    let handle = spawn_chitchat(config, kvs, &UdpTransport).await?;
    node.chitchat_alive.store(true, Ordering::SeqCst);
    info!(
        node_id = %cfg.node_id,
        gossip = %cfg.gossip_advertise_addr(),
        rpc = %cfg.rpc_advertise_addr(),
        seeds = ?cfg.seed_nodes,
        "chitchat started"
    );

    // Liveness: flag when the gossip server task terminates.
    let term = handle.termination_watcher();
    let n = node.clone();
    tokio::spawn(async move {
        let res = term.await;
        n.chitchat_alive.store(false, Ordering::SeqCst);
        if let Err(e) = res {
            warn!("chitchat terminated: {e}");
        }
    });

    // Routing plane: rebuild the ArcSwap snapshot on every membership change.
    let mut watcher = handle.chitchat().lock().await.live_nodes_watcher();
    let n = node.clone();
    tokio::spawn(async move {
        loop {
            let snapshot = watcher.borrow_and_update().clone();
            let m = membership_from(&snapshot);
            info!(
                live = m.live_count,
                active = ?m.active.iter().map(|p| &*p.node_id).collect::<Vec<_>>(),
                "membership updated"
            );
            n.router.update(m);
            n.on_membership_update();
            if watcher.changed().await.is_err() {
                break;
            }
        }
    });
    Ok(handle)
}

pub fn membership_from(states: &BTreeMap<ChitchatId, NodeState>) -> Membership {
    Membership::from_live_nodes(states.iter().map(|(id, st)| LiveNode {
        node_id: id.node_id.to_string(),
        generation_id: id.generation_id,
        rpc_addr: st.get(KEY_RPC_ADDR).map(str::to_owned),
        state: st.get(KEY_STATE).map(str::to_owned),
        az: st.get(KEY_AZ).map(str::to_owned),
    }))
}

pub async fn set_draining(handle: &ChitchatHandle) {
    handle
        .with_chitchat(|c| c.self_node_state().set(KEY_STATE, STATE_DRAINING))
        .await;
}
