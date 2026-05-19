use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum AgentError {
    #[error("agent configuration error: {0}")]
    Configuration(String),
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("provider request timed out during {stage}")]
    Timeout { stage: String },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("upstream returned non-success status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("rate limited by provider")]
    RateLimited { retry_after_ms: Option<u64> },
    #[error("deserialization error: {0}")]
    Deserialize(String),
    #[error("stream terminated before completion")]
    StreamTerminated,
    #[error("non-retryable provider failure: {0}")]
    NonRetryable(String),
}

impl AgentError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Timeout { .. } => true,
            Self::Transport(_) => true,
            Self::RateLimited { .. } => true,
            Self::HttpStatus { status, .. } => *status == 429 || (500..=599).contains(status),
            _ => false,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "CONFIGURATION_ERROR",
            Self::ProviderUnavailable(_) => "PROVIDER_UNAVAILABLE",
            Self::Timeout { .. } => "TIMEOUT",
            Self::Transport(_) => "TRANSPORT_ERROR",
            Self::HttpStatus { .. } => "HTTP_STATUS_ERROR",
            Self::RateLimited { .. } => "RATE_LIMITED",
            Self::Deserialize(_) => "DESERIALIZE_ERROR",
            Self::StreamTerminated => "STREAM_TERMINATED",
            Self::NonRetryable(_) => "NON_RETRYABLE",
        }
    }
}
