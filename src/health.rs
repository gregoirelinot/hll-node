//! /healthz, /readyz and /metrics on the metrics port.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;

use crate::node::Node;

pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(node)
}

async fn healthz(State(n): State<Arc<Node>>) -> impl IntoResponse {
    if n.is_live() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    }
}

async fn readyz(State(n): State<Arc<Node>>) -> impl IntoResponse {
    if n.is_ready() {
        (StatusCode::OK, "ready".to_owned())
    } else {
        let m = n.router.membership();
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "not ready: draining={} live_nodes={} min_quorum={}",
                n.is_draining(),
                m.live_count,
                n.cfg.min_quorum
            ),
        )
    }
}

async fn metrics(State(n): State<Arc<Node>>) -> impl IntoResponse {
    n.refresh_gauges();
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        n.metrics.render(),
    )
}
