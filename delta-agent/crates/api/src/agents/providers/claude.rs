use std::time::Instant;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::json;

use crate::agents::client::{AgentClient, AgentHttpClient};
use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::retry::execute_with_retry;
use crate::agents::types::{
    AgentProvider, AgentRequest, AgentResult, AgentStream, AgentStreamEvent, AgentUsage,
};

#[derive(Debug, Clone)]
pub struct ClaudeClient {
    config: AgentRuntimeConfig,
    http: AgentHttpClient,
}

impl ClaudeClient {
    pub fn new(config: AgentRuntimeConfig, http: AgentHttpClient) -> Self {
        Self { config, http }
    }

    async fn complete_once(
        &self,
        request: AgentRequest,
        _attempt: u32,
    ) -> Result<AgentResult, AgentError> {
        let api_key =
            self.config.anthropic_api_key.clone().ok_or_else(|| {
                AgentError::Configuration("ANTHROPIC_API_KEY is not set".to_owned())
            })?;
        let model = request
            .model_override
            .clone()
            .unwrap_or_else(|| self.config.anthropic_model.clone());
        let endpoint = format!(
            "{}/v1/messages",
            self.config.anthropic_base_url.trim_end_matches('/')
        );

        let mut messages = request
            .context
            .iter()
            .map(|message| {
                json!({
                    "role": normalize_claude_role(&message.role),
                    "content": message.content,
                })
            })
            .collect::<Vec<_>>();
        messages.push(json!({
            "role": "user",
            "content": request.prompt,
        }));

        let payload = json!({
            "model": model,
            "max_tokens": request.max_tokens.unwrap_or(1024),
            "temperature": request.temperature.unwrap_or(0.2),
            "messages": messages,
        });

        let started = Instant::now();
        let response = self
            .http
            .inner()
            .post(endpoint)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|err| AgentError::Transport(err.to_string()))?;
        let response = AgentHttpClient::ensure_success(response).await?;
        let payload: ClaudeResponse = response
            .json()
            .await
            .map_err(|err| AgentError::Deserialize(err.to_string()))?;
        let text = payload
            .content
            .iter()
            .filter_map(|chunk| match chunk {
                ClaudeContent::Text { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        if text.trim().is_empty() {
            return Err(AgentError::Deserialize(
                "anthropic response did not include text content".to_owned(),
            ));
        }
        let usage = payload.usage.map(|usage| AgentUsage {
            input_tokens: usage.input_tokens.unwrap_or(0),
            output_tokens: usage.output_tokens.unwrap_or(0),
            total_tokens: usage
                .input_tokens
                .unwrap_or(0)
                .saturating_add(usage.output_tokens.unwrap_or(0)),
        });

        Ok(AgentResult {
            provider: AgentProvider::Claude,
            model: payload.model.unwrap_or(model),
            text,
            finish_reason: payload.stop_reason,
            usage,
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

impl AgentClient for ClaudeClient {
    fn provider(&self) -> AgentProvider {
        AgentProvider::Claude
    }

    fn complete(&self, request: AgentRequest) -> BoxFuture<'_, Result<AgentResult, AgentError>> {
        Box::pin(async move {
            let policy = self.config.retry_policy();
            execute_with_retry(&policy, |attempt| {
                let req = request.clone();
                async move { self.complete_once(req, attempt).await }
            })
            .await
        })
    }

    fn stream(&self, _request: AgentRequest) -> BoxFuture<'_, Result<AgentStream, AgentError>> {
        Box::pin(async {
            let _placeholder: Option<AgentStreamEvent> = None;
            Err(AgentError::NonRetryable(
                "claude streaming client is not wired in phase B".to_owned(),
            ))
        })
    }
}

fn normalize_claude_role(role: &str) -> &str {
    match role.to_ascii_lowercase().as_str() {
        "assistant" => "assistant",
        _ => "user",
    }
}

#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    content: Vec<ClaudeContent>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClaudeContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}
