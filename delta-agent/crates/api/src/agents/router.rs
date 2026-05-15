use crate::agents::client::{AgentClient, AgentHttpClient, DisabledAgentClient};
use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::providers::{claude::ClaudeClient, deepseek::DeepSeekClient};
use crate::agents::types::{AgentProvider, AgentRequest, AgentResult, AgentStream};

#[derive(Debug, Clone)]
pub struct RealAgentRouter {
    config: AgentRuntimeConfig,
    http: AgentHttpClient,
}

impl RealAgentRouter {
    pub fn new(config: AgentRuntimeConfig) -> Result<Self, AgentError> {
        let http = AgentHttpClient::new(&config)?;
        Ok(Self { config, http })
    }

    pub async fn complete(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentResult, AgentError> {
        if !self.config.use_real_agents {
            let disabled = DisabledAgentClient::new(provider);
            return disabled.complete(request).await;
        }
        match provider {
            AgentProvider::Claude => {
                let client = ClaudeClient::new(self.config.clone(), self.http.clone());
                client.complete(request).await
            }
            AgentProvider::DeepSeek => {
                let client = DeepSeekClient::new(self.config.clone(), self.http.clone());
                client.complete(request).await
            }
        }
    }

    pub async fn stream(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentStream, AgentError> {
        if !self.config.use_real_agents {
            let disabled = DisabledAgentClient::new(provider);
            return disabled.stream(request).await;
        }
        match provider {
            AgentProvider::Claude => {
                let client = ClaudeClient::new(self.config.clone(), self.http.clone());
                client.stream(request).await
            }
            AgentProvider::DeepSeek => {
                let client = DeepSeekClient::new(self.config.clone(), self.http.clone());
                client.stream(request).await
            }
        }
    }
}
