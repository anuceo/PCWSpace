use thiserror::Error;

#[derive(Debug, Error)]
pub enum PcwError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Workspace not found: {0}")]
    WorkspaceNotFound(String),

    #[error("Artifact not found: {0}")]
    ArtifactNotFound(String),

    #[error("DeltaShot not found: {0}")]
    DeltaShotNotFound(String),

    #[error("Workflow not found: {0}")]
    WorkflowNotFound(String),

    #[error("Workflow definition not found: {0}")]
    WorkflowDefNotFound(String),

    #[error("Tamper detected in DeltaShot {shot_id}: {reason}")]
    TamperDetected { shot_id: String, reason: String },

    #[error("Replay failed: {0}")]
    ReplayFailed(String),

    #[error("Encryption key not found for session {0} (may have expired)")]
    EncryptionKeyNotFound(String),

    #[error("Encryption/decryption failed: {0}")]
    EncryptionFailed(String),

    #[error("HMAC signature verification failed for DeltaShot {0}")]
    SignatureVerificationFailed(String),

    #[error("Invalid workflow transition: {from} → {to}")]
    InvalidTransition { from: String, to: String },

    #[error("Agent error [{agent}]: {reason}")]
    AgentError { agent: String, reason: String },

    #[error("Notion sync error: {0}")]
    NotionSyncError(String),

    #[error("Redis error: {0}")]
    RedisError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

pub type PcwResult<T> = Result<T, PcwError>;
