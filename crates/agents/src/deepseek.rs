use pcw_core::{
    config::get_settings,
    errors::{PcwError, PcwResult},
    models::{AgentResult, AgentType, Message},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

#[derive(Debug, Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    max_tokens: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenAiMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
    usage: OpenAiUsage,
    model: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    total_tokens: u32,
}

pub struct DeepSeekAgent {
    client: reqwest::Client,
}

impl DeepSeekAgent {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    #[instrument(skip(self, messages), fields(model = %get_settings().deepseek_model))]
    pub async fn call(
        &self,
        messages: &[Message],
        system_prompt: Option<&str>,
    ) -> PcwResult<AgentResult> {
        let cfg = get_settings();

        let mut openai_messages: Vec<OpenAiMessage> = Vec::new();
        if let Some(sys) = system_prompt {
            openai_messages.push(OpenAiMessage {
                role: "system".into(),
                content: sys.to_string(),
            });
        }
        for m in messages {
            openai_messages.push(OpenAiMessage {
                role: m.role.clone(),
                content: m.content.clone(),
            });
        }

        let body = OpenAiRequest {
            model: cfg.deepseek_model.clone(),
            messages: openai_messages,
            max_tokens: 4096,
        };

        debug!("Calling DeepSeek API");

        let url = format!("{}/v1/chat/completions", cfg.deepseek_base_url);
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", cfg.deepseek_api_key))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| PcwError::AgentError {
                agent: "deepseek".into(),
                reason: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PcwError::AgentError {
                agent: "deepseek".into(),
                reason: format!("HTTP {status}: {text}"),
            });
        }

        let ds_resp: OpenAiResponse = resp.json().await.map_err(|e| PcwError::AgentError {
            agent: "deepseek".into(),
            reason: format!("Failed to parse response: {e}"),
        })?;

        let response_text = ds_resp
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .unwrap_or_default();

        Ok(AgentResult {
            response: response_text,
            shot_id: None,
            tokens_used: ds_resp.usage.total_tokens,
            model: ds_resp.model,
            agent_type: AgentType::DeepSeek,
        })
    }
}

impl Default for DeepSeekAgent {
    fn default() -> Self {
        Self::new()
    }
}
