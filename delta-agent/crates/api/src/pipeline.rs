use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use delta_core::redis_schema::{
    validate_spec_integrity, RedisKeyspace, EVENT_TYPE_ARTIFACT_UPDATE, EVENT_TYPE_MESSAGE_APPEND,
    EVENT_TYPE_STATE_UPDATE,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::info;

const RECENT_MESSAGE_WINDOW: usize = 20;
const SESSION_LOCK_TTL_SECS: u64 = 5;
const SESSION_LOCK_RETRIES: usize = 3;
const LOCK_RETRY_DELAY_MS: u64 = 40;

static PIPELINE: OnceLock<ExecutionPipeline> = OnceLock::new();

#[derive(Debug, Clone, Error)]
pub enum PipelineError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("session lock unavailable")]
    LockUnavailable,
    #[error("serialization failure: {0}")]
    Serialization(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl PipelineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "INVALID_INPUT",
            Self::NotFound(_) => "NOT_FOUND",
            Self::LockUnavailable => "SESSION_LOCKED",
            Self::Serialization(_) => "SERIALIZATION_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(self, Self::LockUnavailable | Self::Internal(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageMode {
    Chat,
    Workflow,
    Execution,
}

impl Default for MessageMode {
    fn default() -> Self {
        Self::Chat
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWorkspaceRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub workspace_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequestV1 {
    pub content: String,
    #[serde(default)]
    pub mode: MessageMode,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackRequest {
    pub target_deltashot_id: String,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactWriteRequest {
    pub session_id: String,
    #[serde(rename = "type")]
    pub artifact_type: String,
    pub content: String,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStartRequest {
    pub session_id: String,
    pub workflow_id: String,
    #[serde(default)]
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStepRequest {
    #[serde(default = "default_workflow_step")]
    pub step: String,
    #[serde(default)]
    pub payload: Value,
}

fn default_workflow_step() -> String {
    "next".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForceAgentRequest {
    pub agent: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceCreateResponse {
    pub workspace_id: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceView {
    pub workspace_id: String,
    pub name: String,
    pub created_at: u64,
    pub owner_id: String,
    pub active_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionCreateResponse {
    pub session_id: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionView {
    pub session_id: String,
    pub status: String,
    pub workflow_id: Option<String>,
    pub state_version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageEnvelope {
    pub id: String,
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaShotEnvelope {
    pub id: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateSummary {
    pub goal: Option<String>,
    pub step: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateEnvelope {
    pub version: u64,
    pub summary: StateSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactEnvelope {
    pub artifact_id: String,
    pub version: u64,
    #[serde(rename = "type")]
    pub artifact_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowEnvelope {
    pub active: bool,
    pub step: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SendMessageResponseV1 {
    pub message: MessageEnvelope,
    pub deltashot: DeltaShotEnvelope,
    pub state: StateEnvelope,
    pub artifacts: Vec<ArtifactEnvelope>,
    pub workflow: WorkflowEnvelope,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionStateResponse {
    pub state: Value,
    pub version: u64,
    pub last_deltashot: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageRecord {
    pub id: String,
    pub role: String,
    pub content: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltashotView {
    pub id: String,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub diff: Value,
    pub hash: String,
    pub prev_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RollbackResponse {
    pub status: String,
    pub current_state_version: u64,
    pub new_deltashot_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactVersionView {
    pub version: u64,
    pub content: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactView {
    pub artifact_id: String,
    pub session_id: String,
    #[serde(rename = "type")]
    pub artifact_type: String,
    pub current_version: u64,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowStateView {
    pub workflow_id: String,
    pub status: String,
    pub session_id: Option<String>,
    pub current_step: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentSelectionView {
    pub session_id: String,
    pub agent: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentLogEntry {
    pub ts: u64,
    pub selected_agent: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateTransitionView {
    pub event_id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: Value,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionTrace {
    pub messages: Vec<MessageRecord>,
    pub deltashots: Vec<DeltashotView>,
    pub state_transitions: Vec<StateTransitionView>,
    pub artifacts: Vec<ArtifactEnvelope>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiResponseMeta {
    pub request_id: String,
    pub timestamp: u64,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiEnvelope<T: Serialize> {
    #[serde(flatten)]
    pub data: T,
    pub meta: ApiResponseMeta,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorEnvelope {
    pub error: ApiErrorBody,
    pub meta: ApiResponseMeta,
}

#[derive(Debug, Clone)]
struct WorkspaceRecord {
    workspace_id: String,
    name: String,
    created_at: u64,
    owner_id: String,
    active_session_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SessionState {
    goal: Option<String>,
    step: Option<String>,
    last_user_message: Option<String>,
    last_agent_message: Option<String>,
    version: u64,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    session_id: String,
    workspace_id: String,
    name: String,
    created_at: u64,
    status: String,
    workflow_id: Option<String>,
    state: SessionState,
    messages: Vec<ChatMessage>,
    events: Vec<SessionEvent>,
    hashchain: Vec<String>,
    deltashot_ids: Vec<String>,
    artifacts: HashSet<String>,
    forced_agent: Option<ForcedAgent>,
}

impl SessionRecord {
    fn new(session_id: String, workspace_id: String, name: String) -> Self {
        Self {
            session_id,
            workspace_id,
            name,
            created_at: now_ms(),
            status: "active".to_owned(),
            workflow_id: None,
            state: SessionState::default(),
            messages: Vec::new(),
            events: Vec::new(),
            hashchain: Vec::new(),
            deltashot_ids: Vec::new(),
            artifacts: HashSet::new(),
            forced_agent: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    id: String,
    role: String,
    content: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEvent {
    id: String,
    event_type: String,
    payload: Value,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct DeltashotRecord {
    id: String,
    session_id: String,
    timestamp: u64,
    event_type: String,
    diff: Value,
    hash: String,
    prev_hash: Option<String>,
    state_snapshot: SessionState,
}

#[derive(Debug, Clone)]
struct ArtifactRecord {
    artifact_id: String,
    session_id: String,
    artifact_type: String,
    created_at: u64,
    current_version: u64,
    versions: BTreeMap<u64, String>,
}

#[derive(Debug, Clone)]
struct WorkflowRecord {
    workflow_id: String,
    session_id: String,
    status: String,
    current_step: String,
    updated_at: u64,
}

#[derive(Debug, Clone)]
struct WorkflowJob {
    workflow_id: String,
    session_id: String,
    step: String,
    payload: String,
    timestamp_ms: u64,
}

#[derive(Debug, Clone)]
struct NotionSyncJob {
    session_id: String,
    summary: String,
    artifacts: Vec<String>,
}

#[derive(Debug, Clone)]
struct ForcedAgent {
    agent: String,
    reason: String,
}

#[derive(Debug, Clone)]
struct SessionContext {
    state: SessionState,
    memory: Vec<ChatMessage>,
    workflow_id: Option<String>,
    forced_agent: Option<ForcedAgent>,
}

#[derive(Debug, Clone, Copy)]
enum AgentKind {
    Claude,
    DeepSeek,
}

#[derive(Debug, Clone)]
struct AgentOutput {
    text: String,
    proposed_goal: Option<String>,
    proposed_step: Option<String>,
    artifact_type: Option<String>,
    artifact_content: Option<String>,
}

#[derive(Debug)]
pub struct ExecutionPipeline {
    keyspace: RedisKeyspace,
    id_counter: AtomicU64,
    workspaces: RwLock<HashMap<String, WorkspaceRecord>>,
    workspace_sessions: RwLock<HashMap<String, Vec<(u64, String)>>>,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    deltashots: RwLock<HashMap<String, DeltashotRecord>>,
    artifacts: RwLock<HashMap<String, ArtifactRecord>>,
    workflows: RwLock<HashMap<String, WorkflowRecord>>,
    workflow_queue: Mutex<Vec<WorkflowJob>>,
    agent_logs: RwLock<HashMap<String, Vec<AgentLogEntry>>>,
    notion_queue: Arc<Mutex<Vec<NotionSyncJob>>>,
    locks: Mutex<HashMap<String, Instant>>,
}

pub type PipelineState = ExecutionPipeline;

impl ExecutionPipeline {
    pub fn new(env: &str) -> Result<Self, PipelineError> {
        let keyspace = RedisKeyspace::new(env)
            .map_err(|err| PipelineError::InvalidInput(format!("invalid env segment: {err}")))?;
        validate_spec_integrity()
            .map_err(|err| PipelineError::InvalidInput(format!("invalid key spec: {err}")))?;

        Ok(Self {
            keyspace,
            id_counter: AtomicU64::new(1),
            workspaces: RwLock::new(HashMap::new()),
            workspace_sessions: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            deltashots: RwLock::new(HashMap::new()),
            artifacts: RwLock::new(HashMap::new()),
            workflows: RwLock::new(HashMap::new()),
            workflow_queue: Mutex::new(Vec::new()),
            agent_logs: RwLock::new(HashMap::new()),
            notion_queue: Arc::new(Mutex::new(Vec::new())),
            locks: Mutex::new(HashMap::new()),
        })
    }

    pub async fn create_workspace(
        &self,
        request: CreateWorkspaceRequest,
    ) -> Result<WorkspaceCreateResponse, PipelineError> {
        if request.name.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "workspace name cannot be empty".to_owned(),
            ));
        }

        let workspace_id = self.next_id("ws");
        let created_at = now_ms();
        let record = WorkspaceRecord {
            workspace_id: workspace_id.clone(),
            name: request.name,
            created_at,
            owner_id: "system".to_owned(),
            active_session_id: None,
        };

        let mut workspaces = self.workspaces.write().await;
        workspaces.insert(workspace_id.clone(), record);

        Ok(WorkspaceCreateResponse {
            workspace_id,
            created_at,
        })
    }

    pub async fn get_workspace(&self, workspace_id: &str) -> Option<WorkspaceView> {
        let workspaces = self.workspaces.read().await;
        let record = workspaces.get(workspace_id)?;
        Some(WorkspaceView {
            workspace_id: record.workspace_id.clone(),
            name: record.name.clone(),
            created_at: record.created_at,
            owner_id: record.owner_id.clone(),
            active_session_id: record.active_session_id.clone(),
        })
    }

    pub async fn list_workspace_sessions(
        &self,
        workspace_id: &str,
        limit: usize,
        cursor: Option<u64>,
    ) -> Vec<SessionView> {
        let idx = self.workspace_sessions.read().await;
        let mut refs = idx.get(workspace_id).cloned().unwrap_or_default();
        refs.sort_by(|a, b| b.0.cmp(&a.0));

        let refs = if let Some(cursor_ts) = cursor {
            refs.into_iter()
                .filter(|(ts, _)| *ts < cursor_ts)
                .collect::<Vec<_>>()
        } else {
            refs
        };

        let ids = refs
            .into_iter()
            .take(limit.max(1))
            .map(|(_, sid)| sid)
            .collect::<Vec<_>>();

        let sessions = self.sessions.read().await;
        ids.iter()
            .filter_map(|session_id| {
                sessions.get(session_id).map(|record| SessionView {
                    session_id: record.session_id.clone(),
                    status: record.status.clone(),
                    workflow_id: record.workflow_id.clone(),
                    state_version: record.state.version,
                })
            })
            .collect::<Vec<_>>()
    }

    pub async fn create_session(
        &self,
        request: CreateSessionRequest,
    ) -> Result<SessionCreateResponse, PipelineError> {
        if request.name.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "session name cannot be empty".to_owned(),
            ));
        }

        {
            let workspaces = self.workspaces.read().await;
            if !workspaces.contains_key(&request.workspace_id) {
                return Err(PipelineError::NotFound(format!(
                    "workspace '{}' not found",
                    request.workspace_id
                )));
            }
        }

        let session_id = self.next_id("sess");
        let created_at = now_ms();
        let record = SessionRecord::new(
            session_id.clone(),
            request.workspace_id.clone(),
            request.name,
        );

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), record);
        }

        {
            let mut links = self.workspace_sessions.write().await;
            links
                .entry(request.workspace_id.clone())
                .or_default()
                .push((created_at, session_id.clone()));
        }

        {
            let mut workspaces = self.workspaces.write().await;
            if let Some(ws) = workspaces.get_mut(&request.workspace_id) {
                ws.active_session_id = Some(session_id.clone());
            }
        }

        Ok(SessionCreateResponse {
            session_id,
            created_at,
        })
    }

    pub async fn handle_message_v1(
        &self,
        session_id: &str,
        request: SendMessageRequestV1,
    ) -> Result<SendMessageResponseV1, PipelineError> {
        if request.content.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "message content cannot be empty".to_owned(),
            ));
        }

        let lock_key = self.acquire_session_lock(session_id).await?;
        let result = self.run_locked_message_pipeline(session_id, request).await;
        self.release_lock(&lock_key).await;
        result
    }

    async fn run_locked_message_pipeline(
        &self,
        session_id: &str,
        request: SendMessageRequestV1,
    ) -> Result<SendMessageResponseV1, PipelineError> {
        let context = self.hydrate_context(session_id).await;

        let user_message = self
            .append_message(session_id, "user", &request.content)
            .await?;

        let (agent, reason) = select_agent(
            &request.content,
            &request.mode,
            context.forced_agent.as_ref(),
        );
        self.append_agent_log(session_id, &agent, &reason).await;

        let agent_output = run_agent(agent, &request.content, &context, &request.mode);

        let prev_state = context.state.clone();
        let next_state = apply_state_mutation(&prev_state, &request.content, &agent_output);
        let diff = compute_diff(&prev_state, &next_state);

        let state_event = self
            .append_event(session_id, EVENT_TYPE_STATE_UPDATE, diff.clone())
            .await?;

        let deltashot = self
            .create_deltashot(
                session_id,
                EVENT_TYPE_STATE_UPDATE,
                state_event.payload.clone(),
                next_state.clone(),
            )
            .await?;

        let mut artifacts = Vec::new();
        if let (Some(artifact_type), Some(content)) = (
            agent_output.artifact_type.clone(),
            agent_output.artifact_content.clone(),
        ) {
            let artifact = self
                .create_or_update_artifact_internal(session_id, &artifact_type, &content)
                .await?;
            artifacts.push(artifact.clone());

            let _ = self
                .append_event(
                    session_id,
                    EVENT_TYPE_ARTIFACT_UPDATE,
                    serde_json::json!({
                        "artifact_id": artifact.artifact_id,
                        "version": artifact.version,
                        "type": artifact.artifact_type,
                    }),
                )
                .await?;
        }

        let agent_message = self
            .append_message(session_id, "agent", &agent_output.text)
            .await?;

        let _ = self
            .append_event(
                session_id,
                EVENT_TYPE_MESSAGE_APPEND,
                serde_json::json!({
                    "role": "agent",
                    "message_id": agent_message.id,
                    "content": agent_output.text,
                }),
            )
            .await?;

        self.persist_state(session_id, next_state.clone()).await?;

        let workflow_step = next_state
            .step
            .clone()
            .unwrap_or_else(|| "continue".to_owned());
        let workflow_active =
            matches!(request.mode, MessageMode::Workflow | MessageMode::Execution)
                || context.workflow_id.is_some();

        if workflow_active {
            let workflow_id = context
                .workflow_id
                .clone()
                .unwrap_or_else(|| format!("wf-{}", session_id));
            self.enqueue_workflow(&workflow_id, session_id, &workflow_step, Value::Null)
                .await;
            self.bind_session_workflow(session_id, &workflow_id).await;
        }

        self.enqueue_notion_sync(session_id, &agent_output.text, &artifacts)
            .await;

        info!(
            session_id = %session_id,
            state_key = %self
                .keyspace
                .session_state(session_id)
                .unwrap_or_else(|_| "invalid".to_owned()),
            messages_key = %self
                .keyspace
                .session_messages(session_id)
                .unwrap_or_else(|_| "invalid".to_owned()),
            user_message_id = %user_message.id,
            deltashot_id = %deltashot.id,
            "deterministic session message pipeline completed"
        );

        Ok(SendMessageResponseV1 {
            message: MessageEnvelope {
                id: agent_message.id,
                role: "agent".to_owned(),
                content: agent_output.text,
            },
            deltashot: DeltaShotEnvelope {
                id: deltashot.id,
                timestamp: deltashot.timestamp,
            },
            state: StateEnvelope {
                version: next_state.version,
                summary: StateSummary {
                    goal: next_state.goal,
                    step: next_state.step,
                },
            },
            artifacts,
            workflow: WorkflowEnvelope {
                active: workflow_active,
                step: workflow_step,
            },
        })
    }

    pub async fn get_or_create_session_view(&self, session_id: &str) -> SessionView {
        let mut sessions = self.sessions.write().await;
        let record = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });

        SessionView {
            session_id: record.session_id.clone(),
            status: record.status.clone(),
            workflow_id: record.workflow_id.clone(),
            state_version: record.state.version,
        }
    }

    pub async fn get_session_state(&self, session_id: &str) -> Option<SessionStateResponse> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        Some(SessionStateResponse {
            state: serde_json::json!({
                "goal": session.state.goal,
                "step": session.state.step,
                "last_user_message": session.state.last_user_message,
                "last_agent_message": session.state.last_agent_message,
            }),
            version: session.state.version,
            last_deltashot: session.deltashot_ids.last().cloned(),
        })
    }

    pub async fn get_session_messages(
        &self,
        session_id: &str,
        limit: usize,
        cursor: Option<String>,
    ) -> Vec<MessageRecord> {
        let sessions = self.sessions.read().await;
        let Some(session) = sessions.get(session_id) else {
            return Vec::new();
        };

        let mut messages = session.messages.clone();
        if let Some(cursor_id) = cursor {
            if let Some(index) = messages.iter().position(|m| m.id == cursor_id) {
                messages.truncate(index);
            }
        }

        messages
            .into_iter()
            .rev()
            .take(limit.max(1))
            .map(|m| MessageRecord {
                id: m.id,
                role: m.role,
                content: m.content,
                timestamp: m.timestamp,
            })
            .collect::<Vec<_>>()
    }

    pub async fn list_deltashots(&self, session_id: &str) -> Vec<DeltashotView> {
        let sessions = self.sessions.read().await;
        let Some(session) = sessions.get(session_id) else {
            return Vec::new();
        };
        let ids = session.deltashot_ids.clone();
        drop(sessions);

        let map = self.deltashots.read().await;
        ids.iter()
            .filter_map(|id| map.get(id))
            .map(|ds| DeltashotView {
                id: ds.id.clone(),
                timestamp: ds.timestamp,
                event_type: ds.event_type.clone(),
                diff: ds.diff.clone(),
                hash: ds.hash.clone(),
                prev_hash: ds.prev_hash.clone(),
            })
            .collect::<Vec<_>>()
    }

    pub async fn get_deltashot(&self, deltashot_id: &str) -> Option<DeltashotView> {
        let map = self.deltashots.read().await;
        let ds = map.get(deltashot_id)?;
        Some(DeltashotView {
            id: ds.id.clone(),
            timestamp: ds.timestamp,
            event_type: ds.event_type.clone(),
            diff: ds.diff.clone(),
            hash: ds.hash.clone(),
            prev_hash: ds.prev_hash.clone(),
        })
    }

    pub async fn rollback_session(
        &self,
        session_id: &str,
        request: RollbackRequest,
    ) -> Result<RollbackResponse, PipelineError> {
        let lock_key = self.acquire_session_lock(session_id).await?;

        let result = async {
            let target_id = request.target_deltashot_id.clone();
            let mode = request.mode.to_ascii_lowercase();

            let target = {
                let deltas = self.deltashots.read().await;
                deltas.get(&target_id).cloned().ok_or_else(|| {
                    PipelineError::NotFound(format!("deltashot '{}' not found", target_id))
                })?
            };

            let mut sessions = self.sessions.write().await;
            let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
                SessionRecord::new(
                    session_id.to_owned(),
                    "adhoc".to_owned(),
                    "Adhoc Session".to_owned(),
                )
            });

            let prev = session.state.clone();
            session.state = target.state_snapshot.clone();

            if mode == "hard" {
                if let Some(idx) = session.deltashot_ids.iter().position(|id| id == &target_id) {
                    session.deltashot_ids.truncate(idx + 1);
                    session.hashchain.truncate(idx + 1);
                }
            }

            drop(sessions);

            let rollback_diff = compute_diff(&prev, &target.state_snapshot);
            let rollback_event = self
                .append_event(session_id, "ROLLBACK", rollback_diff)
                .await?;

            let rollback_ds = self
                .create_deltashot(
                    session_id,
                    "ROLLBACK",
                    rollback_event.payload,
                    target.state_snapshot.clone(),
                )
                .await?;

            Ok::<RollbackResponse, PipelineError>(RollbackResponse {
                status: "rolled_back".to_owned(),
                current_state_version: target.state_snapshot.version,
                new_deltashot_id: rollback_ds.id,
            })
        }
        .await;

        self.release_lock(&lock_key).await;
        result
    }

    pub async fn create_or_update_artifact_api(
        &self,
        request: ArtifactWriteRequest,
    ) -> Result<ArtifactEnvelope, PipelineError> {
        self.create_or_update_artifact_internal(
            &request.session_id,
            &request.artifact_type,
            &request.content,
        )
        .await
    }

    pub async fn get_artifact(&self, artifact_id: &str) -> Option<ArtifactView> {
        let artifacts = self.artifacts.read().await;
        let record = artifacts.get(artifact_id)?;
        Some(ArtifactView {
            artifact_id: record.artifact_id.clone(),
            session_id: record.session_id.clone(),
            artifact_type: record.artifact_type.clone(),
            current_version: record.current_version,
            created_at: record.created_at,
        })
    }

    pub async fn get_artifact_versions(&self, artifact_id: &str) -> Vec<ArtifactVersionView> {
        let artifacts = self.artifacts.read().await;
        let Some(record) = artifacts.get(artifact_id) else {
            return Vec::new();
        };
        record
            .versions
            .iter()
            .map(|(version, content)| ArtifactVersionView {
                version: *version,
                content: content.clone(),
            })
            .collect::<Vec<_>>()
    }

    pub async fn get_artifact_version(
        &self,
        artifact_id: &str,
        version: u64,
    ) -> Option<ArtifactVersionView> {
        let artifacts = self.artifacts.read().await;
        let record = artifacts.get(artifact_id)?;
        let content = record.versions.get(&version)?;
        Some(ArtifactVersionView {
            version,
            content: content.clone(),
        })
    }

    pub async fn start_workflow(
        &self,
        request: WorkflowStartRequest,
    ) -> Result<WorkflowStateView, PipelineError> {
        let now = now_ms();
        let record = WorkflowRecord {
            workflow_id: request.workflow_id.clone(),
            session_id: request.session_id.clone(),
            status: "active".to_owned(),
            current_step: "start".to_owned(),
            updated_at: now,
        };

        {
            let mut workflows = self.workflows.write().await;
            workflows.insert(request.workflow_id.clone(), record.clone());
        }

        self.enqueue_workflow(
            &request.workflow_id,
            &request.session_id,
            "start",
            request.input,
        )
        .await;
        self.bind_session_workflow(&request.session_id, &request.workflow_id)
            .await;

        Ok(WorkflowStateView {
            workflow_id: record.workflow_id,
            status: record.status,
            session_id: Some(record.session_id),
            current_step: record.current_step,
        })
    }

    pub async fn get_workflow_state(&self, workflow_id: &str) -> Option<WorkflowStateView> {
        let workflows = self.workflows.read().await;
        let record = workflows.get(workflow_id)?;
        Some(WorkflowStateView {
            workflow_id: record.workflow_id.clone(),
            status: record.status.clone(),
            session_id: Some(record.session_id.clone()),
            current_step: record.current_step.clone(),
        })
    }

    pub async fn advance_workflow_step(
        &self,
        workflow_id: &str,
        request: WorkflowStepRequest,
    ) -> Result<WorkflowStateView, PipelineError> {
        let session_id = {
            let mut workflows = self.workflows.write().await;
            let record = workflows.get_mut(workflow_id).ok_or_else(|| {
                PipelineError::NotFound(format!("workflow '{}' not found", workflow_id))
            })?;
            record.current_step = request.step.clone();
            record.updated_at = now_ms();
            record.session_id.clone()
        };

        self.enqueue_workflow(workflow_id, &session_id, &request.step, request.payload)
            .await;

        Ok(WorkflowStateView {
            workflow_id: workflow_id.to_owned(),
            status: "active".to_owned(),
            session_id: Some(session_id),
            current_step: request.step,
        })
    }

    pub async fn force_agent_selection(
        &self,
        session_id: &str,
        request: ForceAgentRequest,
    ) -> Result<AgentSelectionView, PipelineError> {
        let canonical = normalize_agent_name(&request.agent).ok_or_else(|| {
            PipelineError::InvalidInput("agent must be one of: claude | deepseek".to_owned())
        })?;

        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        session.forced_agent = Some(ForcedAgent {
            agent: canonical.to_owned(),
            reason: request.reason.clone(),
        });

        Ok(AgentSelectionView {
            session_id: session_id.to_owned(),
            agent: canonical.to_owned(),
            reason: request.reason,
        })
    }

    pub async fn get_agent_logs(&self, session_id: &str) -> Vec<AgentLogEntry> {
        let logs = self.agent_logs.read().await;
        logs.get(session_id).cloned().unwrap_or_default()
    }

    pub async fn get_trace(&self, session_id: &str) -> ExecutionTrace {
        let session_opt = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        let Some(session) = session_opt else {
            return ExecutionTrace {
                messages: Vec::new(),
                deltashots: Vec::new(),
                state_transitions: Vec::new(),
                artifacts: Vec::new(),
            };
        };

        let messages = session
            .messages
            .iter()
            .map(|m| MessageRecord {
                id: m.id.clone(),
                role: m.role.clone(),
                content: m.content.clone(),
                timestamp: m.timestamp,
            })
            .collect::<Vec<_>>();

        let state_transitions = session
            .events
            .iter()
            .map(|e| StateTransitionView {
                event_id: e.id.clone(),
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                timestamp: e.timestamp,
            })
            .collect::<Vec<_>>();

        let artifacts = {
            let map = self.artifacts.read().await;
            session
                .artifacts
                .iter()
                .filter_map(|artifact_id| map.get(artifact_id))
                .map(|record| ArtifactEnvelope {
                    artifact_id: record.artifact_id.clone(),
                    version: record.current_version,
                    artifact_type: record.artifact_type.clone(),
                })
                .collect::<Vec<_>>()
        };

        ExecutionTrace {
            messages,
            deltashots: self.list_deltashots(session_id).await,
            state_transitions,
            artifacts,
        }
    }

    pub fn health(&self) -> HealthResponse {
        HealthResponse {
            status: "ok".to_owned(),
        }
    }

    async fn hydrate_context(&self, session_id: &str) -> SessionContext {
        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });

        let mut memory = session.messages.clone();
        memory.reverse();
        memory.truncate(RECENT_MESSAGE_WINDOW);

        SessionContext {
            state: session.state.clone(),
            memory,
            workflow_id: session.workflow_id.clone(),
            forced_agent: session.forced_agent.clone(),
        }
    }

    async fn acquire_session_lock(&self, session_id: &str) -> Result<String, PipelineError> {
        let lock_key = self
            .keyspace
            .lock_session(session_id)
            .map_err(|err| PipelineError::InvalidInput(err.to_string()))?;

        for _ in 0..SESSION_LOCK_RETRIES {
            if self.try_acquire_lock(&lock_key).await {
                return Ok(lock_key);
            }
            tokio::time::sleep(Duration::from_millis(LOCK_RETRY_DELAY_MS)).await;
        }
        Err(PipelineError::LockUnavailable)
    }

    async fn try_acquire_lock(&self, lock_key: &str) -> bool {
        let mut locks = self.locks.lock().await;
        let now = Instant::now();
        locks.retain(|_, expires_at| *expires_at > now);

        if locks.contains_key(lock_key) {
            return false;
        }

        locks.insert(
            lock_key.to_owned(),
            now + Duration::from_secs(SESSION_LOCK_TTL_SECS),
        );
        true
    }

    async fn release_lock(&self, lock_key: &str) {
        let mut locks = self.locks.lock().await;
        locks.remove(lock_key);
    }

    async fn append_message(
        &self,
        session_id: &str,
        role: &str,
        content: &str,
    ) -> Result<ChatMessage, PipelineError> {
        let message = ChatMessage {
            id: self.next_id("msg"),
            role: role.to_owned(),
            content: content.to_owned(),
            timestamp: now_ms(),
        };

        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        session.messages.push(message.clone());

        Ok(message)
    }

    async fn append_event(
        &self,
        session_id: &str,
        event_type: &str,
        payload: Value,
    ) -> Result<SessionEvent, PipelineError> {
        let event = SessionEvent {
            id: self.next_id("evt"),
            event_type: event_type.to_owned(),
            payload,
            timestamp: now_ms(),
        };

        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        session.events.push(event.clone());
        Ok(event)
    }

    async fn create_deltashot(
        &self,
        session_id: &str,
        event_type: &str,
        diff: Value,
        state_snapshot: SessionState,
    ) -> Result<DeltashotRecord, PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });

        let id = self.next_id("ds");
        let timestamp = now_ms();
        let prev_hash = session.hashchain.last().cloned();

        let digest_input = serde_json::json!({
            "id": id,
            "session_id": session_id,
            "timestamp": timestamp,
            "event_type": event_type,
            "prev_hash": prev_hash,
            "diff": diff,
        });
        let encoded = serde_json::to_vec(&digest_input)
            .map_err(|err| PipelineError::Serialization(err.to_string()))?;
        let hash = blake3::hash(&encoded).to_hex().to_string();

        let record = DeltashotRecord {
            id: id.clone(),
            session_id: session_id.to_owned(),
            timestamp,
            event_type: event_type.to_owned(),
            diff: digest_input["diff"].clone(),
            hash: hash.clone(),
            prev_hash: session.hashchain.last().cloned(),
            state_snapshot,
        };

        session.hashchain.push(hash);
        session.deltashot_ids.push(id.clone());
        drop(sessions);

        let mut all = self.deltashots.write().await;
        all.insert(id, record.clone());
        Ok(record)
    }

    async fn create_or_update_artifact_internal(
        &self,
        session_id: &str,
        artifact_type: &str,
        content: &str,
    ) -> Result<ArtifactEnvelope, PipelineError> {
        let mut artifacts = self.artifacts.write().await;
        let artifact_id = format!("art_{}", session_id);
        let record = artifacts
            .entry(artifact_id.clone())
            .or_insert_with(|| ArtifactRecord {
                artifact_id: artifact_id.clone(),
                session_id: session_id.to_owned(),
                artifact_type: artifact_type.to_owned(),
                created_at: now_ms(),
                current_version: 0,
                versions: BTreeMap::new(),
            });

        record.current_version += 1;
        record
            .versions
            .insert(record.current_version, content.to_owned());

        {
            let mut sessions = self.sessions.write().await;
            let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
                SessionRecord::new(
                    session_id.to_owned(),
                    "adhoc".to_owned(),
                    "Adhoc Session".to_owned(),
                )
            });
            session.artifacts.insert(artifact_id.clone());
        }

        Ok(ArtifactEnvelope {
            artifact_id,
            version: record.current_version,
            artifact_type: record.artifact_type.clone(),
        })
    }

    async fn persist_state(
        &self,
        session_id: &str,
        next_state: SessionState,
    ) -> Result<(), PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        session.state = next_state;
        Ok(())
    }

    async fn enqueue_workflow(
        &self,
        workflow_id: &str,
        session_id: &str,
        step: &str,
        payload: Value,
    ) {
        let now = now_ms();
        {
            let mut queue = self.workflow_queue.lock().await;
            queue.push(WorkflowJob {
                workflow_id: workflow_id.to_owned(),
                session_id: session_id.to_owned(),
                step: step.to_owned(),
                payload: payload.to_string(),
                timestamp_ms: now,
            });
        }

        {
            let mut workflows = self.workflows.write().await;
            workflows.insert(
                workflow_id.to_owned(),
                WorkflowRecord {
                    workflow_id: workflow_id.to_owned(),
                    session_id: session_id.to_owned(),
                    status: "active".to_owned(),
                    current_step: step.to_owned(),
                    updated_at: now,
                },
            );
        }
    }

    async fn bind_session_workflow(&self, session_id: &str, workflow_id: &str) {
        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        session.workflow_id = Some(workflow_id.to_owned());
    }

    async fn append_agent_log(&self, session_id: &str, agent: &AgentKind, reason: &str) {
        let entry = AgentLogEntry {
            ts: now_ms(),
            selected_agent: match agent {
                AgentKind::Claude => "claude",
                AgentKind::DeepSeek => "deepseek",
            }
            .to_owned(),
            reason: reason.to_owned(),
        };

        let mut logs = self.agent_logs.write().await;
        logs.entry(session_id.to_owned()).or_default().push(entry);
    }

    async fn enqueue_notion_sync(
        &self,
        session_id: &str,
        summary: &str,
        artifacts: &[ArtifactEnvelope],
    ) {
        let job = NotionSyncJob {
            session_id: session_id.to_owned(),
            summary: summary.to_owned(),
            artifacts: artifacts
                .iter()
                .map(|artifact| artifact.artifact_id.clone())
                .collect::<Vec<_>>(),
        };

        let queue = Arc::clone(&self.notion_queue);
        let session_id = session_id.to_owned();
        tokio::spawn(async move {
            let mut q = queue.lock().await;
            q.push(job);
            info!(session_id = %session_id, "notion sync enqueued");
        });
    }

    fn next_id(&self, prefix: &str) -> String {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{n}")
    }

    #[cfg(test)]
    async fn replay_state_version(&self, session_id: &str) -> Option<u64> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        let updates = session
            .events
            .iter()
            .filter(|event| event.event_type == EVENT_TYPE_STATE_UPDATE)
            .count() as u64;
        Some(updates)
    }
}

pub fn shared_pipeline() -> &'static ExecutionPipeline {
    PIPELINE.get_or_init(|| {
        let env = std::env::var("DELTA_AGENT_ENV").unwrap_or_else(|_| "dev".to_owned());
        ExecutionPipeline::new(&env).expect("shared pipeline initialization must succeed")
    })
}

fn normalize_agent_name(input: &str) -> Option<&'static str> {
    match input.to_ascii_lowercase().as_str() {
        "claude" => Some("claude"),
        "deepseek" => Some("deepseek"),
        _ => None,
    }
}

fn select_agent(
    input: &str,
    mode: &MessageMode,
    forced: Option<&ForcedAgent>,
) -> (AgentKind, String) {
    if let Some(forced) = forced {
        let kind = if forced.agent == "deepseek" {
            AgentKind::DeepSeek
        } else {
            AgentKind::Claude
        };
        return (kind, format!("forced: {}", forced.reason));
    }

    match mode {
        MessageMode::Execution => return (AgentKind::DeepSeek, "mode=execution".to_owned()),
        MessageMode::Workflow => return (AgentKind::Claude, "mode=workflow".to_owned()),
        MessageMode::Chat => {}
    }

    let lower = input.to_ascii_lowercase();
    if lower.contains("code")
        || lower.contains("rust")
        || lower.contains("implement")
        || lower.contains("compile")
    {
        (AgentKind::DeepSeek, "intent=technical".to_owned())
    } else {
        (AgentKind::Claude, "intent=reasoning".to_owned())
    }
}

fn run_agent(
    agent: AgentKind,
    input: &str,
    context: &SessionContext,
    mode: &MessageMode,
) -> AgentOutput {
    let complexity = estimate_complexity(input);
    let memory_hint = context.memory.len();
    let prefix = match agent {
        AgentKind::Claude => "reasoning",
        AgentKind::DeepSeek => "technical",
    };

    let response_text = format!(
        "[{prefix}:{complexity}:{:?}] processed '{}' with {} memory items",
        mode, input, memory_hint
    );

    let lower = input.to_ascii_lowercase();
    let proposed_goal = if lower.contains("finalize") {
        Some("finalize landing page".to_owned())
    } else if lower.contains("draft") {
        Some("draft landing page".to_owned())
    } else {
        context.state.goal.clone()
    };

    let proposed_step = if lower.contains("refine") || lower.contains("finalize") {
        Some("refine".to_owned())
    } else if lower.contains("draft") {
        Some("draft".to_owned())
    } else {
        Some("continue".to_owned())
    };

    let (artifact_type, artifact_content) = if lower.contains("artifact")
        || lower.contains("code")
        || matches!(mode, MessageMode::Execution)
    {
        (
            Some("code".to_owned()),
            Some(format!("artifact payload for '{input}'")),
        )
    } else {
        (None, None)
    };

    AgentOutput {
        text: response_text,
        proposed_goal,
        proposed_step,
        artifact_type,
        artifact_content,
    }
}

fn estimate_complexity(input: &str) -> &'static str {
    match input.split_whitespace().count() {
        0..=12 => "low",
        13..=35 => "medium",
        _ => "high",
    }
}

fn apply_state_mutation(
    prev: &SessionState,
    user_input: &str,
    output: &AgentOutput,
) -> SessionState {
    let mut next = prev.clone();
    next.version = prev.version + 1;
    next.last_user_message = Some(user_input.to_owned());
    next.last_agent_message = Some(output.text.clone());
    next.goal = output.proposed_goal.clone().or_else(|| prev.goal.clone());
    next.step = output.proposed_step.clone().or_else(|| prev.step.clone());
    next
}

fn compute_diff(prev: &SessionState, next: &SessionState) -> Value {
    let mut changes = serde_json::Map::new();
    maybe_insert_change(
        &mut changes,
        "goal",
        prev.goal.clone().unwrap_or_default(),
        next.goal.clone().unwrap_or_default(),
    );
    maybe_insert_change(
        &mut changes,
        "step",
        prev.step.clone().unwrap_or_default(),
        next.step.clone().unwrap_or_default(),
    );
    maybe_insert_change(
        &mut changes,
        "last_user_message",
        prev.last_user_message.clone().unwrap_or_default(),
        next.last_user_message.clone().unwrap_or_default(),
    );
    maybe_insert_change(
        &mut changes,
        "last_agent_message",
        prev.last_agent_message.clone().unwrap_or_default(),
        next.last_agent_message.clone().unwrap_or_default(),
    );
    maybe_insert_change(
        &mut changes,
        "version",
        prev.version.to_string(),
        next.version.to_string(),
    );

    serde_json::json!({
        "type": EVENT_TYPE_STATE_UPDATE,
        "changes": changes,
    })
}

fn maybe_insert_change(
    changes: &mut serde_json::Map<String, Value>,
    field: &str,
    from: String,
    to: String,
) {
    if from != to {
        changes.insert(
            field.to_owned(),
            serde_json::json!({
                "from": from,
                "to": to,
            }),
        );
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deterministic_pipeline_creates_replayable_state_updates() {
        let pipeline = ExecutionPipeline::new("test").expect("pipeline should initialize");

        let ws = pipeline
            .create_workspace(CreateWorkspaceRequest {
                name: "Workspace".to_owned(),
            })
            .await
            .expect("workspace should create");

        let session = pipeline
            .create_session(CreateSessionRequest {
                workspace_id: ws.workspace_id,
                name: "Session".to_owned(),
            })
            .await
            .expect("session should create");

        let response = pipeline
            .handle_message_v1(
                &session.session_id,
                SendMessageRequestV1 {
                    content: "draft artifact and refine".to_owned(),
                    mode: MessageMode::Workflow,
                    metadata: Value::Null,
                },
            )
            .await
            .expect("pipeline should succeed");

        assert_eq!(response.state.version, 1);
        assert_eq!(response.artifacts.len(), 1);

        let replay_version = pipeline
            .replay_state_version(&session.session_id)
            .await
            .expect("session should exist");
        assert_eq!(replay_version, response.state.version);
    }

    #[test]
    fn agent_routing_picks_technical_for_code_intent() {
        let (agent, _) = select_agent("please implement rust code", &MessageMode::Chat, None);
        assert!(matches!(agent, AgentKind::DeepSeek));

        let (agent, _) = select_agent("help me reason", &MessageMode::Chat, None);
        assert!(matches!(agent, AgentKind::Claude));
    }
}
