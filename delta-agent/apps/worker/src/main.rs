mod config;
mod scheduler_loop;

#[tokio::main]
async fn main() {
    let config_path = std::env::var("DELTA_AGENT_CONFIG")
        .unwrap_or_else(|_| "../../configs/default.toml".to_owned());
    let worker_config = match config::load(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("worker config load failed from {}: {error:?}", config_path);
            std::process::exit(1);
        }
    };
    let worker_config = apply_env_overrides(worker_config);

    scheduler_loop::run(worker_config).await;
    if let Err(error) = Ok::<(), anyhow::Error>(()) {
        eprintln!("worker failed: {error:?}");
        std::process::exit(1);
    }
}

fn apply_env_overrides(mut config: config::WorkerConfig) -> config::WorkerConfig {
    if let Ok(base_url) = std::env::var("WORKER_API_BASE_URL") {
        let trimmed = base_url.trim();
        if !trimmed.is_empty() {
            config.api_base_url = trimmed.to_owned();
        }
    }

    if let Ok(value) = std::env::var("WORKER_POLL_INTERVAL_MS") {
        if let Ok(parsed) = value.trim().parse::<u64>() {
            config.poll_interval_ms = parsed;
        }
    }

    if let Ok(value) = std::env::var("WORKER_MAX_CONCURRENCY") {
        if let Ok(parsed) = value.trim().parse::<usize>() {
            config.max_concurrency = parsed.max(1);
        }
    }

    if let Ok(value) = std::env::var("WORKER_REQUEST_TIMEOUT_MS") {
        if let Ok(parsed) = value.trim().parse::<u64>() {
            config.request_timeout_ms = parsed;
        }
    }

    if let Ok(value) = std::env::var("WORKER_NOTION_POLL_ENABLED") {
        let normalized = value.trim().to_ascii_lowercase();
        if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
            config.notion_poll_enabled = true;
        } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
            config.notion_poll_enabled = false;
        }
    }

    if config.api_key.is_none() {
        config.api_key = std::env::var("DELTA_AGENT_API_KEY")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("PCW_API_KEY")
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
            });
    }

    config
}
