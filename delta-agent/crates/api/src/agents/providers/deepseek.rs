use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::agents::client::{AgentClient, AgentHttpClient};
use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::retry::execute_with_retry;
use crate::agents::types::{
    AgentProvider, AgentRequest, AgentResult, AgentStream, AgentStreamEvent, AgentUsage,
};

#[derive(Debug, Clone)]
pub struct DeepSeekClient {
    config: AgentRuntimeConfig,
    http: AgentHttpClient,
}

impl DeepSeekClient {
    pub fn new(config: AgentRuntimeConfig, http: AgentHttpClient) -> Self {
        Self { config, http }
    }

    async fn complete_once(
        &self,
        request: AgentRequest,
        _attempt: u32,
    ) -> Result<AgentResult, AgentError> {
        let api_key =
            self.config.deepseek_api_key.clone().ok_or_else(|| {
                AgentError::Configuration("DEEPSEEK_API_KEY is not set".to_owned())
            })?;
        let model = request
            .model_override
            .clone()
            .unwrap_or_else(|| self.config.deepseek_model.clone());
        let endpoint = format!(
            "{}/v1/chat/completions",
            self.config.deepseek_base_url.trim_end_matches('/')
        );

        let mut messages = request
            .context
            .iter()
            .map(|message| {
                json!({
                    "role": normalize_chat_role(&message.role),
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
            "messages": messages,
            "temperature": request.temperature.unwrap_or(0.2),
            "max_tokens": request.max_tokens.unwrap_or(1024),
            "stream": false,
        });

        let started = Instant::now();
        let response = self
            .http
            .inner()
            .post(endpoint)
            .header("authorization", format!("Bearer {api_key}"))
            .header("content-type", "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|err| AgentError::Transport(err.to_string()))?;
        let response = AgentHttpClient::ensure_success(response).await?;
        let payload: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|err| AgentError::Deserialize(err.to_string()))?;
        let first_choice = payload.choices.first().ok_or_else(|| {
            AgentError::Deserialize("deepseek response had no choices".to_owned())
        })?;
        let text = first_choice.message.content.clone().ok_or_else(|| {
            AgentError::Deserialize("deepseek response choice had no message content".to_owned())
        })?;
        let usage = payload.usage.map(|usage| AgentUsage {
            input_tokens: usage.prompt_tokens.unwrap_or(0),
            output_tokens: usage.completion_tokens.unwrap_or(0),
            total_tokens: usage.total_tokens.unwrap_or(
                usage
                    .prompt_tokens
                    .unwrap_or(0)
                    .saturating_add(usage.completion_tokens.unwrap_or(0)),
            ),
        });

        Ok(AgentResult {
            provider: AgentProvider::DeepSeek,
            model: payload.model.unwrap_or(model),
            text,
            finish_reason: first_choice.finish_reason.clone(),
            usage,
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

impl AgentClient for DeepSeekClient {
    fn provider(&self) -> AgentProvider {
        AgentProvider::DeepSeek
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

    fn stream(&self, request: AgentRequest) -> BoxFuture<'_, Result<AgentStream, AgentError>> {
        Box::pin(async move {
            let api_key = self.config.deepseek_api_key.clone().ok_or_else(|| {
                AgentError::Configuration("DEEPSEEK_API_KEY is not set".to_owned())
            })?;
            let model = request
                .model_override
                .clone()
                .unwrap_or_else(|| self.config.deepseek_model.clone());
            let endpoint = format!(
                "{}/v1/chat/completions",
                self.config.deepseek_base_url.trim_end_matches('/')
            );
            let mut messages = request
                .context
                .iter()
                .map(|message| {
                    json!({
                        "role": normalize_chat_role(&message.role),
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
                "messages": messages,
                "temperature": request.temperature.unwrap_or(0.2),
                "max_tokens": request.max_tokens.unwrap_or(1024),
                "stream": true,
            });

            let response = self
                .http
                .inner()
                .post(endpoint)
                .header("authorization", format!("Bearer {api_key}"))
                .header("content-type", "application/json")
                .json(&payload)
                .send()
                .await
                .map_err(|err| AgentError::Transport(err.to_string()))?;
            let response = AgentHttpClient::ensure_success(response).await?;

            let (tx, rx) = mpsc::channel::<Result<AgentStreamEvent, AgentError>>(64);
            let idle_timeout = self.config.stream_idle_timeout();
            tokio::spawn(async move {
                let mut stream = response.bytes_stream();
                let mut buffer = String::new();
                loop {
                    let next_chunk = match tokio::time::timeout(idle_timeout, stream.next()).await {
                        Ok(item) => item,
                        Err(_) => {
                            let _ = tx
                                .send(Err(AgentError::Timeout {
                                    stage: "stream_idle".to_owned(),
                                }))
                                .await;
                            return;
                        }
                    };
                    let Some(chunk_result) = next_chunk else {
                        let _ = tx.send(Err(AgentError::StreamTerminated)).await;
                        return;
                    };
                    let chunk = match chunk_result {
                        Ok(chunk) => chunk,
                        Err(err) => {
                            let _ = tx.send(Err(AgentError::Transport(err.to_string()))).await;
                            return;
                        }
                    };
                    buffer.push_str(&String::from_utf8_lossy(&chunk));
                    while let Some(newline_idx) = buffer.find('\n') {
                        let line = buffer[..newline_idx].trim_end_matches('\r').to_owned();
                        buffer.drain(..=newline_idx);
                        if !line.starts_with("data:") {
                            continue;
                        }
                        let data = line[5..].trim();
                        if data.is_empty() {
                            continue;
                        }
                        if data == "[DONE]" {
                            let _ = tx
                                .send(Ok(AgentStreamEvent::Done {
                                    model: model.clone(),
                                    finish_reason: None,
                                }))
                                .await;
                            return;
                        }
                        let parsed: Value = match serde_json::from_str(data) {
                            Ok(value) => value,
                            Err(err) => {
                                let _ =
                                    tx.send(Err(AgentError::Deserialize(err.to_string()))).await;
                                return;
                            }
                        };
                        if let Some(usage) = parsed.get("usage").cloned() {
                            let usage = AgentUsage {
                                input_tokens: usage
                                    .get("prompt_tokens")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0),
                                output_tokens: usage
                                    .get("completion_tokens")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0),
                                total_tokens: usage
                                    .get("total_tokens")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0),
                            };
                            let _ = tx.send(Ok(AgentStreamEvent::Usage { usage })).await;
                        }
                        if let Some(delta) = parsed
                            .pointer("/choices/0/delta/content")
                            .and_then(Value::as_str)
                        {
                            let _ = tx
                                .send(Ok(AgentStreamEvent::Token {
                                    delta: delta.to_owned(),
                                }))
                                .await;
                        }
                        if let Some(finish_reason) = parsed
                            .pointer("/choices/0/finish_reason")
                            .and_then(Value::as_str)
                        {
                            let _ = tx
                                .send(Ok(AgentStreamEvent::Done {
                                    model: model.clone(),
                                    finish_reason: Some(finish_reason.to_owned()),
                                }))
                                .await;
                            return;
                        }
                    }
                }
            });
            let stream: AgentStream = Box::pin(ReceiverStream::new(rx));
            Ok(stream)
        })
    }
}

fn normalize_chat_role(role: &str) -> &str {
    match role.to_ascii_lowercase().as_str() {
        "system" => "system",
        "assistant" => "assistant",
        _ => "user",
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    #[serde(default)]
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}
