use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() {
    // Determine .env path — look next to binary, then cwd
    let env_path = find_env_file();

    // Run startup checks: network → key prompting → live verification
    api::startup::run_checks(&env_path).await;

    // Init structured logging AFTER startup (startup uses plain println for UX)
    infra::logging::init_logging();

    // Settings are now fully populated (startup may have set env vars)
    let cfg = pcw_core::config::get_settings();
    let addr: SocketAddr = format!("{}:{}", cfg.pcw_host, cfg.pcw_port)
        .parse()
        .expect("Invalid host/port");

    let router = api::routes::build_router();

    info!(%addr, "PCW server starting");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind — is the port already in use?");

    // Try Storm first; fall back to the in-process Redis poller if unavailable.
    let redis_url    = cfg.redis_url.clone();
    let nimbus_url   = std::env::var("STORM_NIMBUS_URL")
        .unwrap_or_else(|_| "http://storm-nimbus:8080".to_string());
    let worker_bin   = std::env::var("PCW_STORM_WORKER_BIN")
        .unwrap_or_else(|_| "pcw-storm-worker".to_string());

    let storm_active = runtime::scheduler::submit_storm_topology(&nimbus_url, &worker_bin).await;

    if !storm_active {
        tokio::spawn(async move {
            if let Err(e) = runtime::scheduler::run_workflow_worker_loop(&redis_url, 1).await {
                tracing::error!(error = %e, "Workflow worker loop exited");
            }
        });
    }

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");
}

fn find_env_file() -> String {
    // 1. Explicit override via env var
    if let Ok(p) = std::env::var("PCW_ENV_FILE") {
        return p;
    }
    // 2. .env next to the executable
    if let Ok(mut exe) = std::env::current_exe() {
        exe.pop();
        exe.push(".env");
        if exe.exists() {
            return exe.to_string_lossy().into_owned();
        }
    }
    // 3. .env in current working directory (default)
    ".env".to_string()
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    tracing::info!("Shutdown signal received — stopping server");
}
