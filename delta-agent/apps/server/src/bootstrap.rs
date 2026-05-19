use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context;
use tracing::{info, Level};

use crate::config::{load, AppConfig};

pub async fn run() -> anyhow::Result<()> {
    init_tracing();

    let config_path = resolve_config_path();
    let config = load(&config_path)?;
    apply_runtime_env_overrides(&config);

    let pipeline = api::pipeline::ExecutionPipeline::new(&config.runtime.env)?;
    let app = api::router_with_pipeline(pipeline);
    let addr = format!("{}:{}", config.server.host, config.server.port)
        .parse::<SocketAddr>()
        .context("invalid server bind address")?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind server listener")?;

    info!(address = %addr, config_path, "server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum server failed")?;

    info!("server shutdown complete");
    Ok(())
}

fn apply_runtime_env_overrides(config: &AppConfig) {
    set_if_missing("DELTA_AGENT_ENV", Some(config.runtime.env.clone()));
    set_if_missing(
        "DELTA_AGENT_REDIS_URL",
        sanitize_optional_string(config.persistence.redis_url.as_deref()),
    );
    set_if_missing(
        "DELTA_AGENT_VDDAB_ROOT",
        sanitize_optional_string(config.persistence.vddab_root.as_deref()),
    );

    set_if_missing(
        "DELTA_AGENT_AUTH_REQUIRED",
        config.auth.required.map(bool_to_string),
    );
    set_if_missing(
        "DELTA_AGENT_AUTH_DISABLED",
        config.auth.disabled.map(bool_to_string),
    );
    set_if_missing(
        "DELTA_AGENT_API_KEYS",
        join_values_or_none(&config.auth.writer_keys),
    );
    set_if_missing(
        "DELTA_AGENT_READONLY_API_KEYS",
        join_values_or_none(&config.auth.reader_keys),
    );
    set_if_missing(
        "DELTA_AGENT_ADMIN_API_KEYS",
        join_values_or_none(&config.auth.admin_keys),
    );

    set_if_missing(
        "DELTA_AGENT_USE_REAL_AGENTS",
        config.agents.use_real_agents.map(bool_to_string),
    );
    set_if_missing(
        "ANTHROPIC_API_KEY",
        sanitize_optional_string(config.agents.anthropic_api_key.as_deref()),
    );
    set_if_missing(
        "ANTHROPIC_MODEL",
        sanitize_optional_string(config.agents.anthropic_model.as_deref()),
    );
    set_if_missing(
        "DEEPSEEK_API_KEY",
        sanitize_optional_string(config.agents.deepseek_api_key.as_deref()),
    );
    set_if_missing(
        "DEEPSEEK_MODEL",
        sanitize_optional_string(config.agents.deepseek_model.as_deref()),
    );

    set_if_missing(
        "NOTION_SYNC_ENABLED",
        config.notion.enabled.map(bool_to_string),
    );
    set_if_missing(
        "NOTION_TOKEN",
        sanitize_optional_string(config.notion.token.as_deref()),
    );
    set_if_missing(
        "NOTION_DATABASE_ID",
        sanitize_optional_string(config.notion.database_id.as_deref()),
    );
    set_if_missing(
        "NOTION_PARENT_PAGE_ID",
        sanitize_optional_string(config.notion.parent_page_id.as_deref()),
    );
    set_if_missing(
        "NOTION_DATABASE_TITLE_PROPERTY",
        sanitize_optional_string(config.notion.title_property.as_deref()),
    );
    set_if_missing(
        "NOTION_API_BASE_URL",
        sanitize_optional_string(config.notion.api_base_url.as_deref()),
    );
    set_if_missing(
        "NOTION_API_VERSION",
        sanitize_optional_string(config.notion.api_version.as_deref()),
    );
    set_if_missing(
        "NOTION_TIMEOUT_MS",
        config.notion.timeout_ms.map(|value| value.to_string()),
    );
    set_if_missing(
        "NOTION_SYNC_MAX_ATTEMPTS",
        config.notion.max_attempts.map(|value| value.to_string()),
    );
}

fn set_if_missing(key: &str, value: Option<String>) {
    if std::env::var_os(key).is_some() {
        return;
    }
    let Some(value) = value else {
        return;
    };
    std::env::set_var(key, value);
}

fn sanitize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn join_values_or_none(values: &[String]) -> Option<String> {
    let joined = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn bool_to_string(value: bool) -> String {
    if value {
        "true".to_owned()
    } else {
        "false".to_owned()
    }
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { info!("received Ctrl+C, initiating graceful shutdown"); }
        _ = sigterm => { info!("received SIGTERM, initiating graceful shutdown"); }
    }
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
