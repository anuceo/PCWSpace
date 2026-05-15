use std::pin::Pin;

use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agents::errors::AgentError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentProvider {
    Claude,
    DeepSeek,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentMode {
    #[default]
    Chat,
    Workflow,
    Execution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRequest {
    pub session_id: String,
    pub request_id: String,
    pub prompt: String,
    #[serde(default)]
    pub context: Vec<AgentMessage>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub mode: AgentMode,
    #[serde(default)]
    pub model_override: Option<String>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResult {
    pub provider: AgentProvider,
    pub model: String,
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<AgentUsage>,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentStreamEvent {
    Token {
        delta: String,
    },
    Usage {
        usage: AgentUsage,
    },
    Done {
        model: String,
        finish_reason: Option<String>,
    },
}

pub type AgentStream =
    Pin<Box<dyn Stream<Item = Result<AgentStreamEvent, AgentError>> + Send + 'static>>;
