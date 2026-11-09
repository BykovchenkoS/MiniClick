//! Запуск MiniClick и корректная остановка по SIGINT/SIGTERM.

use clap::Parser;
use server::{
    config::{Cli, Config},
    AppState,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
    let config = Config::load(Cli::parse(), &std::env::current_dir()?)?;
    let address = config.node.listen_addr.clone();
    let state = tokio::task::spawn_blocking(move || AppState::open(config)).await??;
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let mut sync_task = None;
    if let replication::Role::Replica {
        leader_addr,
        leader_id,
    } = &state.config.node.role
    {
        let leader_addr = leader_addr.clone();
        let leader_id = leader_id.clone();
        let expected = types::NodeIdentity {
            cluster_id: state.config.node.cluster_id.clone(),
            node_id: leader_id.clone(),
        };
        let interval = std::time::Duration::from_millis(state.config.node.sync_interval_ms);
        let catalog = state.catalog.clone();
        let (stop, stopped) = std::sync::mpsc::channel();
        tracing::info!(%leader_addr, %leader_id, "Реплика синхронизируется с лидером");
        sync_task = Some((
            stop,
            tokio::task::spawn_blocking(move || {
                server::replica::sync_loop(catalog, leader_addr, expected, interval, stopped);
            }),
        ));
    }
    tracing::info!(address = %listener.local_addr()?, node_id = %state.config.node.node_id, "MiniClick запущен");
    let served = axum::serve(listener, server::router(state))
        .with_graceful_shutdown(shutdown())
        .await;
    if let Some((stop, task)) = sync_task {
        let _ = stop.send(());
        match task.await {
            Ok(()) => tracing::info!("Фоновая синхронизация остановлена"),
            Err(error) => tracing::error!(%error, "Фоновая синхронизация завершилась с ошибкой"),
        }
    }
    served?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            }
            Err(error) => {
                tracing::error!(%error, "Не удалось установить обработчик SIGTERM");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("MiniClick завершает текущие запросы");
}
