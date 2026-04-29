use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::HashMap;

// ── helpers ────────────────────────────────────────────────────────────────

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

// ── Enums ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Closed,
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactType {
    Code,
    Doc,
    Design,
    Dataset,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Created,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentType {
    Claude,
    DeepSeek,
}

impl std::fmt::Display for AgentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentType::Claude => write!(f, "claude"),
            AgentType::DeepSeek => write!(f, "deepseek"),
        }
    }
}

// ── Workspace ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub workspace_id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub active_session_id: Option<String>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Workspace {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            workspace_id: new_id(),
            name: name.into(),
            created_at: now(),
            active_session_id: None,
            metadata: HashMap::new(),
        }
    }
}

// ── Session ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String, // "user" | "assistant" | "system"
    pub content: String,
    pub agent_type: Option<AgentType>,
    pub timestamp: DateTime<Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Message {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: content.into(),
            agent_type: None,
            timestamp: now(),
            metadata: HashMap::new(),
        }
    }

    pub fn assistant(content: impl Into<String>, agent_type: AgentType) -> Self {
        Self {
            role: "assistant".into(),
            content: content.into(),
            agent_type: Some(agent_type),
            timestamp: now(),
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub workspace_id: String,
    pub status: SessionStatus,
    pub messages: Vec<Message>,
    pub state_pointer: Option<String>,
    pub workflow_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Session {
    pub fn new(workspace_id: impl Into<String>) -> Self {
        Self {
            session_id: new_id(),
            workspace_id: workspace_id.into(),
            status: SessionStatus::Active,
            messages: Vec::new(),
            state_pointer: None,
            workflow_id: None,
            created_at: now(),
            closed_at: None,
            metadata: HashMap::new(),
        }
    }

    pub fn to_state_object(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

// ── Artifact ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub artifact_id: String,
    pub name: String,
    pub artifact_type: ArtifactType,
    pub content: String,
    pub content_hash: String,
    pub version: u32,
    pub parent_version_id: Option<String>,
    pub linked_session: Option<String>,
    pub agent_type: Option<AgentType>,
    pub deltashot_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

// ── DeltaShot ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaShot {
    pub deltashot_id: String,
    pub session_id: String,
    pub sequence_number: u64,
    pub prev_hash: String,       // SHA-256 hex of previous shot
    pub content_hash: String,    // SHA-256(prev_hash || encrypted_payload)
    pub diff_payload: Vec<u8>,   // AES-256-GCM encrypted diff (serialized as base64 in Redis)
    pub hmac_signature: Vec<u8>, // HMAC-SHA256 of content_hash
    pub action: String,
    pub agent_type: Option<AgentType>,
    pub message_index: Option<u64>,
    pub artifact_changes: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

// ── Workflow ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub name: String,
    pub handler: String,
    pub agent_type: Option<AgentType>,
    pub prompt: Option<String>,
    pub timeout_secs: u64,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub workflow_def_id: String,
    pub name: String,
    pub steps: Vec<WorkflowStep>,
    pub state_transitions: HashMap<String, Vec<String>>,
    pub created_at: DateTime<Utc>,
}

impl WorkflowDefinition {
    pub fn new_linear(name: impl Into<String>, steps: Vec<WorkflowStep>) -> Self {
        let mut transitions = HashMap::new();
        for i in 0..steps.len().saturating_sub(1) {
            transitions.insert(steps[i].name.clone(), vec![steps[i + 1].name.clone()]);
        }
        Self {
            workflow_def_id: new_id(),
            name: name.into(),
            steps,
            state_transitions: transitions,
            created_at: now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowState {
    pub workflow_id: String,
    pub workflow_def_id: String,
    pub session_id: String,
    pub status: WorkflowStatus,
    pub current_step: Option<String>,
    pub step_status: StepStatus,
    pub retry_count: u32,
    pub context: HashMap<String, serde_json::Value>,
    pub step_results: HashMap<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl WorkflowState {
    pub fn new(def: &WorkflowDefinition, session_id: impl Into<String>) -> Self {
        let first = def.steps.first().map(|s| s.name.clone());
        Self {
            workflow_id: new_id(),
            workflow_def_id: def.workflow_def_id.clone(),
            session_id: session_id.into(),
            status: WorkflowStatus::Running,
            current_step: first,
            step_status: StepStatus::Pending,
            retry_count: 0,
            context: HashMap::new(),
            step_results: HashMap::new(),
            created_at: now(),
            updated_at: now(),
            completed_at: None,
            error: None,
        }
    }
}

// ── Agent Result ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResult {
    pub response: String,
    pub shot_id: Option<String>,
    pub tokens_used: u32,
    pub model: String,
    pub agent_type: AgentType,
}
