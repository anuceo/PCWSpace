use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() {
    // Load env + init logging
    infra::logging::init_logging();

    let cfg = pcw_core::config::get_settings();
    let addr: SocketAddr = format!("{}:{}", cfg.pcw_host, cfg.pcw_port)
        .parse()
        .expect("Invalid host/port");

    let router = api::routes::build_router();

    info!(%addr, "PCW server starting");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C handler");
    info!("Shutdown signal received");
}
