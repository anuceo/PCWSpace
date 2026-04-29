use pcw_core::{
    errors::PcwResult,
    models::{AgentResult, AgentType, Message},
};
use crate::{claude::ClaudeAgent, deepseek::DeepSeekAgent};

pub struct AgentRouter {
    claude: ClaudeAgent,
    deepseek: DeepSeekAgent,
}

impl AgentRouter {
    pub fn new() -> Self {
        Self {
            claude: ClaudeAgent::new(),
            deepseek: DeepSeekAgent::new(),
        }
    }

    pub async fn call(
        &self,
        agent_type: AgentType,
        messages: &[Message],
        system_prompt: Option<&str>,
    ) -> PcwResult<AgentResult> {
        match agent_type {
            AgentType::Claude => self.claude.call(messages, system_prompt).await,
            AgentType::DeepSeek => self.deepseek.call(messages, system_prompt).await,
        }
    }

    /// Route based on a task hint: code/analysis tasks → DeepSeek, everything else → Claude.
    pub fn select_agent(task_hint: &str) -> AgentType {
        let lower = task_hint.to_lowercase();
        if lower.contains("code")
            || lower.contains("debug")
            || lower.contains("refactor")
            || lower.contains("algorithm")
        {
            AgentType::DeepSeek
        } else {
            AgentType::Claude
        }
    }
}

impl Default for AgentRouter {
    fn default() -> Self {
        Self::new()
    }
}
