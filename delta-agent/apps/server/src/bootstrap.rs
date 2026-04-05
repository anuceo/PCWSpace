use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context;
use tracing::{info, Level};

use crate::config::load;

pub async fn run() -> anyhow::Result<()> {
    init_tracing();

    let config_path = resolve_config_path();
    let config = load(&config_path)?;

    let app = api::router();
    let addr = format!("{}:{}", config.host, config.port)
        .parse::<SocketAddr>()
        .context("invalid server bind address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind server listener")?;

    info!(address = %addr, config_path, "server listening");
    axum::serve(listener, app)
        .await
        .context("axum server failed")?;

    Ok(())
}

fn resolve_config_path() -> String {
    if let Ok(path) = std::env::var("DELTA_AGENT_CONFIG") {
        return path;
    }

    for candidate in ["configs/default.toml", "../../configs/default.toml"] {
        if Path::new(candidate).exists() {
            return candidate.to_owned();
        }
    }

    "configs/default.toml".to_owned()
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "server=info".into()),
        )
        .try_init();
}
