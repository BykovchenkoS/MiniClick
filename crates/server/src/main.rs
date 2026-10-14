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
    tracing::info!(address = %listener.local_addr()?, node_id = %state.config.node.node_id, "MiniClick запущен");
    axum::serve(listener, server::router(state))
        .with_graceful_shutdown(shutdown())
        .await?;
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
