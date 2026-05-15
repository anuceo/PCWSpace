use anyhow::Context;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde_json::Value;
use tokio::time::{sleep, Duration};
use tracing::{info, warn, Level};

use crate::config::WorkerConfig;

pub async fn run(config: WorkerConfig) {
    init_tracing();
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            panic!("failed to build worker HTTP client: {error}");
        }
    };
    let headers = build_auth_headers(config.api_key.as_deref());

    info!(
        max_concurrency = config.max_concurrency,
        poll_interval_ms = config.poll_interval_ms,
        api_base_url = %config.api_base_url,
        notion_poll_enabled = config.notion_poll_enabled,
        request_timeout_ms = config.request_timeout_ms,
        "worker starting"
    );

    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    loop {
        let mut workflow_executed = 0usize;
        for _ in 0..config.max_concurrency.max(1) {
            match execute_next(
                &client,
                &config.api_base_url,
                "/api/v1/workflows/execute-next",
                &headers,
            )
            .await
            {
                Ok(true) => workflow_executed += 1,
                Ok(false) => break,
                Err(error) => {
                    warn!(error = %error, "workflow execution poll failed");
                    break;
                }
            }
        }
        let notion_executed = if config.notion_poll_enabled {
            match execute_next(
                &client,
                &config.api_base_url,
                "/api/v1/workflows/notion/execute-next",
                &headers,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    warn!(error = %error, "notion execution poll failed");
                    false
                }
            }
        } else {
            false
        };
        info!(
            workflow_executed,
            notion_executed, "worker loop tick complete"
        );
        sleep(poll_interval).await;
    }
}

async fn execute_next(
    client: &reqwest::Client,
    base_url: &str,
    route: &str,
    headers: &HeaderMap,
) -> anyhow::Result<bool> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), route);
    let response = client
        .post(&url)
        .headers(headers.clone())
        .send()
        .await
        .with_context(|| format!("request failed for {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!("{} returned status {} with body {}", url, status, body);
    }
    let value: Value =
        serde_json::from_str(&body).with_context(|| format!("invalid JSON from {}", url))?;
    Ok(value
        .get("executed")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

fn build_auth_headers(api_key: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(api_key) = api_key {
        let token = format!("Bearer {}", api_key.trim());
        if let Ok(value) = HeaderValue::from_str(&token) {
            headers.insert(AUTHORIZATION, value);
        }
    }
    headers
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info".into()),
        )
        .try_init();
}
