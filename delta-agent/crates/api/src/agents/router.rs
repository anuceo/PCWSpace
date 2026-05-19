use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::agents::client::{AgentClient, AgentHttpClient};
use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::providers::{claude::ClaudeClient, deepseek::DeepSeekClient};
use crate::agents::types::{AgentProvider, AgentRequest, AgentResult, AgentStream, AgentStreamEvent};

#[derive(Debug, Clone)]
pub struct RealAgentRouter {
    config: AgentRuntimeConfig,
    http: AgentHttpClient,
    claude_semaphore: Option<Arc<Semaphore>>,
    deepseek_semaphore: Option<Arc<Semaphore>>,
}

impl RealAgentRouter {
    pub fn new(config: AgentRuntimeConfig) -> Result<Self, AgentError> {
        let http = AgentHttpClient::new(&config)?;
        let claude_semaphore = if config.max_concurrent_claude > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_claude)))
        } else {
            None
        };
        let deepseek_semaphore = if config.max_concurrent_deepseek > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_deepseek)))
        } else {
            None
        };
        Ok(Self {
            config,
            http,
            claude_semaphore,
            deepseek_semaphore,
        })
    }

    pub async fn complete(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentResult, AgentError> {
        if !self.config.use_real_agents {
            return Err(AgentError::Configuration(
                "real agents are disabled (set DELTA_AGENT_USE_REAL_AGENTS=true)".to_owned(),
            ));
        }
        let result = self.complete_for_provider(provider, request.clone()).await;
        match result {
            Ok(r) => Ok(r),
            Err(err) if err.is_retryable() => {
                let fallback = match provider {
                    AgentProvider::Claude => AgentProvider::DeepSeek,
                    AgentProvider::DeepSeek => AgentProvider::Claude,
                };
                let fallback_available = match fallback {
                    AgentProvider::Claude => self.config.anthropic_api_key.is_some(),
                    AgentProvider::DeepSeek => self.config.deepseek_api_key.is_some(),
                };
                if fallback_available {
                    self.complete_for_provider(fallback, request).await
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn stream(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentStream, AgentError> {
        if !self.config.use_real_agents {
            return Err(AgentError::Configuration(
                "real agents are disabled (set DELTA_AGENT_USE_REAL_AGENTS=true)".to_owned(),
            ));
        }
        self.stream_for_provider(provider, request).await
    }

    async fn complete_for_provider(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentResult, AgentError> {
        match provider {
            AgentProvider::Claude => {
                let _permit = self.acquire_claude_permit().await?;
                let client = ClaudeClient::new(self.config.clone(), self.http.clone());
                client.complete(request).await
            }
            AgentProvider::DeepSeek => {
                let _permit = self.acquire_deepseek_permit().await?;
                let client = DeepSeekClient::new(self.config.clone(), self.http.clone());
                client.complete(request).await
            }
        }
    }

    async fn stream_for_provider(
        &self,
        provider: AgentProvider,
        request: AgentRequest,
    ) -> Result<AgentStream, AgentError> {
        match provider {
            AgentProvider::Claude => {
                let permit = self.acquire_claude_permit().await?;
                let client = ClaudeClient::new(self.config.clone(), self.http.clone());
                let inner = client.stream(request).await?;
                Ok(Box::pin(PermitStream { inner, _permit: permit }))
            }
            AgentProvider::DeepSeek => {
                let permit = self.acquire_deepseek_permit().await?;
                let client = DeepSeekClient::new(self.config.clone(), self.http.clone());
                let inner = client.stream(request).await?;
                Ok(Box::pin(PermitStream { inner, _permit: permit }))
            }
        }
    }

    async fn acquire_claude_permit(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, AgentError> {
        match &self.claude_semaphore {
            Some(sem) => sem
                .clone()
                .acquire_owned()
                .await
                .map(Some)
                .map_err(|_| AgentError::ProviderUnavailable("claude semaphore closed".to_owned())),
            None => Ok(None),
        }
    }

    async fn acquire_deepseek_permit(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, AgentError> {
        match &self.deepseek_semaphore {
            Some(sem) => sem
                .clone()
                .acquire_owned()
                .await
                .map(Some)
                .map_err(|_| {
                    AgentError::ProviderUnavailable("deepseek semaphore closed".to_owned())
                }),
            None => Ok(None),
        }
    }
}

struct PermitStream {
    inner: AgentStream,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Stream for PermitStream {
    type Item = Result<AgentStreamEvent, AgentError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Unpin for PermitStream {}
