use pcw_core::{
    config::get_settings,
    errors::{PcwError, PcwResult},
    models::{AgentResult, AgentType, Message},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    system: Option<String>,
    messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaudeMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContent>,
    usage: ClaudeUsage,
    model: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeContent {
    #[serde(rename = "type")]
    content_type: String,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    input_tokens: u32,
    output_tokens: u32,
}

pub struct ClaudeAgent {
    client: reqwest::Client,
}

impl ClaudeAgent {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    #[instrument(skip(self, messages), fields(model = %get_settings().claude_model))]
    pub async fn call(
        &self,
        messages: &[Message],
        system_prompt: Option<&str>,
    ) -> PcwResult<AgentResult> {
        let cfg = get_settings();

        let claude_messages: Vec<ClaudeMessage> = messages
            .iter()
            .map(|m| ClaudeMessage {
                role: m.role.clone(),
                content: m.content.clone(),
            })
            .collect();

        let body = ClaudeRequest {
            model: cfg.claude_model.clone(),
            max_tokens: 4096,
            system: system_prompt.map(str::to_string),
            messages: claude_messages,
        };

        debug!("Calling Claude API");

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &cfg.anthropic_api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| PcwError::AgentError {
                agent: "claude".into(),
                reason: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PcwError::AgentError {
                agent: "claude".into(),
                reason: format!("HTTP {status}: {text}"),
            });
        }

        let claude_resp: ClaudeResponse = resp.json().await.map_err(|e| PcwError::AgentError {
            agent: "claude".into(),
            reason: format!("Failed to parse response: {e}"),
        })?;

        let response_text = claude_resp
            .content
            .iter()
            .filter(|c| c.content_type == "text")
            .filter_map(|c| c.text.as_deref())
            .collect::<Vec<_>>()
            .join("");

        let tokens = claude_resp.usage.input_tokens + claude_resp.usage.output_tokens;

        Ok(AgentResult {
            response: response_text,
            shot_id: None,
            tokens_used: tokens,
            model: claude_resp.model,
            agent_type: AgentType::Claude,
        })
    }
}

impl Default for ClaudeAgent {
    fn default() -> Self {
        Self::new()
    }
}
