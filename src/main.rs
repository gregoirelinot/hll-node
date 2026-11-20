use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,chitchat=warn")))
        .with_target(false)
        .init();

    let cfg = hll_node::Config::from_env()?;
    tracing::info!(?cfg, "configuration");
    let handle = hll_node::start(cfg).await?;

    wait_for_shutdown_signal().await;
    handle.drain_and_shutdown().await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = term.recv() => tracing::info!("SIGTERM received"),
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Ctrl-C received");
}
