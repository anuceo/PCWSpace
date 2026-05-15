use futures_util::future::BoxFuture;
use reqwest::Client;

use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::types::{AgentProvider, AgentRequest, AgentResult, AgentStream};

pub trait AgentClient: Send + Sync {
    fn provider(&self) -> AgentProvider;
    fn complete(&self, request: AgentRequest) -> BoxFuture<'_, Result<AgentResult, AgentError>>;
    fn stream(&self, request: AgentRequest) -> BoxFuture<'_, Result<AgentStream, AgentError>>;
}

#[derive(Debug, Clone)]
pub struct AgentHttpClient {
    inner: Client,
}

impl AgentHttpClient {
    pub fn new(config: &AgentRuntimeConfig) -> Result<Self, AgentError> {
        let client = Client::builder()
            .connect_timeout(config.connect_timeout())
            .timeout(config.request_timeout())
            .user_agent("delta-agent/phase-a")
            .build()
            .map_err(|err| AgentError::Configuration(err.to_string()))?;
        Ok(Self { inner: client })
    }

    pub fn inner(&self) -> &Client {
        &self.inner
    }

    pub async fn ensure_success(
        response: reqwest::Response,
    ) -> Result<reqwest::Response, AgentError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        let retry_after_ms = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(|seconds| seconds.saturating_mul(1_000));

        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable upstream body>".to_owned());
        let body = truncate_error_body(body, 2_048);
        if status.as_u16() == 429 {
            return Err(AgentError::RateLimited { retry_after_ms });
        }
        Err(AgentError::HttpStatus {
            status: status.as_u16(),
            body,
        })
    }
}

fn truncate_error_body(mut body: String, max_chars: usize) -> String {
    if body.chars().count() <= max_chars {
        return body;
    }
    body = body.chars().take(max_chars).collect::<String>();
    body.push('…');
    body
}
