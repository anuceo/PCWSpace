use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use delta_core::redis_schema::{
    validate_spec_integrity, RedisKeyspace, EVENT_TYPE_ARTIFACT_UPDATE, EVENT_TYPE_MESSAGE_APPEND,
    EVENT_TYPE_STATE_UPDATE, SNAPSHOT_INTERVAL_EVENTS,
};
use deltashot::{
    apply_ops_to_state, canonical_serialize_ops, compute_chain_hash, compute_diff_ops, DeltaShot,
    DeltaShotMetadata, DeltaShotOp, OpType,
};
use replay::{
    adapters::RedisVddabRepository,
    verifier::{audit_replay, verify_chain, ReplayAuditResult, VerificationResult},
    verifier::{DeltaRepository, StateRepository},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell as TokioOnceCell, RwLock};
use tracing::{info, warn};

const RECENT_MESSAGE_WINDOW: usize = 20;
const SESSION_LOCK_TTL_SECS: u64 = 5;
const SESSION_LOCK_RETRIES: usize = 3;
const LOCK_RETRY_DELAY_MS: u64 = 40;
const EVENT_TYPE_EXECUTION_STEP: &str = "EXECUTION_STEP";
const MAIN_BRANCH_ID: &str = "br_main";

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MessageMode {
    #[default]
    Chat,
    Workflow,
    Execution,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchCreateRequest {
    pub from_deltashot_id: String,
    pub label: String,
    #[serde(default)]
    pub mode: BranchMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchSwitchRequest {
    pub branch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMergeRequest {
    pub source_branch: String,
    pub target_branch: String,
    pub strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessageRequestV1 {
    pub content: String,
    #[serde(default)]
    pub mode: MessageMode,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub metadata: Value,
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
pub struct WorkspaceSessionsResponse {
    pub sessions: Vec<SessionView>,
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
pub struct SessionMessagesResponse {
    pub messages: Vec<MessageRecord>,
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
pub struct SessionDeltashotsResponse {
    pub deltashots: Vec<DeltashotView>,
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
pub struct ArtifactVersionsResponse {
    pub versions: Vec<ArtifactVersionView>,
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
pub struct WorkflowExecutionResult {
    pub workflow_id: String,
    pub session_id: String,
    pub step: String,
    pub deltashot_id: String,
    pub state_version: u64,
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
pub struct AgentLogsResponse {
    pub logs: Vec<AgentLogEntry>,
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
pub struct BranchAuditResponse {
    pub branch_id: String,
    pub valid: bool,
    pub chain: VerificationResult,
    pub replay: ReplayAuditResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BranchMode {
    #[default]
    Soft,
    Hard,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchCreateResponse {
    pub branch: BranchPointer,
    pub state: BranchStateView,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchPointer {
    pub branch_id: String,
    pub parent_deltashot_id: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchStateView {
    pub version: u64,
    pub forked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchListResponse {
    pub branches: Vec<BranchSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchSummary {
    pub branch_id: String,
    pub is_main: bool,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchSwitchResponse {
    pub branch_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchMergeResponse {
    pub status: String,
    pub new_deltashot_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamEventEnvelope {
    pub event: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiResponseMeta {
    pub request_id: String,
    pub timestamp: u64,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiEnvelope<T: Serialize> {
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

#[derive(Debug, Clone, Serialize)]
pub struct StreamAckEvent {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamTokenEvent {
    pub delta: String,
    pub accumulated: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamMessageEvent {
    pub message_id: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamDeltaShotEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamArtifactEvent {
    pub artifact_id: String,
    pub version: u64,
    #[serde(rename = "type")]
    pub artifact_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamWorkflowEvent {
    pub step: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamDoneEvent {
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamErrorEvent {
    pub code: String,
    pub retryable: bool,
}

// ==========================================================
// Section 1: Context structs
// ==========================================================

#[derive(Debug, Clone)]
struct PersistenceConfig {
    redis_url: String,
    vddab_root: String,
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
    _workspace_id: String,
    _name: String,
    created_at: u64,
    status: String,
    workflow_id: Option<String>,
    state: SessionState,
    messages: Vec<ChatMessage>,
    events: Vec<SessionEvent>,
    hashchain: Vec<String>,
    deltashot_ids: Vec<String>,
    artifacts: HashSet<String>,
    active_branch_id: String,
    branches: HashMap<String, BranchRecord>,
    forced_agent: Option<ForcedAgent>,
}

impl SessionRecord {
    fn new(session_id: String, workspace_id: String, name: String) -> Self {
        let mut record = Self {
            session_id,
            _workspace_id: workspace_id,
            _name: name,
            created_at: now_ms(),
            status: "active".to_owned(),
            workflow_id: None,
            state: SessionState::default(),
            messages: Vec::new(),
            events: Vec::new(),
            hashchain: Vec::new(),
            deltashot_ids: Vec::new(),
            artifacts: HashSet::new(),
            active_branch_id: MAIN_BRANCH_ID.to_owned(),
            branches: HashMap::new(),
            forced_agent: None,
        };
        sync_active_branch_from_session(&mut record);
        record
    }
}

#[derive(Debug, Clone)]
struct BranchRecord {
    branch_id: String,
    _session_id: String,
    _parent_deltashot_id: String,
    _created_at: u64,
    label: Option<String>,
    mode: BranchMode,
    state: SessionState,
    messages: Vec<ChatMessage>,
    events: Vec<SessionEvent>,
    hashchain: Vec<String>,
    deltashot_ids: Vec<String>,
    artifacts: HashSet<String>,
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
    branch_id: String,
    timestamp: u64,
    event_type: String,
    ops: Vec<DeltaShotOp>,
    hash: String,
    prev_hash: String,
    state_snapshot: SessionState,
    metadata: DeltaShotMetadata,
}

#[derive(Debug, Clone)]
struct ArtifactRecord {
    artifact_id: String,
    session_id: String,
    branch_id: String,
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

#[derive(Debug, Clone)]
struct ArtifactWriteOutcome {
    envelope: ArtifactEnvelope,
    previous_content: Option<String>,
}

// ==========================================================
// Section 2: Pipeline state + initialization
// ==========================================================

#[derive(Debug)]
pub struct ExecutionPipeline {
    keyspace: RedisKeyspace,
    persistence: Option<PersistenceConfig>,
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
    idempotent_streams: Mutex<HashMap<String, Vec<StreamEventEnvelope>>>,
    persistence_hydration: TokioOnceCell<()>,
}

pub type PipelineState = ExecutionPipeline;

// ==========================================================
// Section 3: Core execution flow (lock -> execute -> commit)
// ==========================================================

impl ExecutionPipeline {
    pub fn new(env: &str) -> Result<Self, PipelineError> {
        let keyspace = RedisKeyspace::new(env)
            .map_err(|err| PipelineError::InvalidInput(format!("invalid env segment: {err}")))?;
        validate_spec_integrity()
            .map_err(|err| PipelineError::InvalidInput(format!("invalid key spec: {err}")))?;

        Ok(Self {
            keyspace,
            persistence: load_persistence_config(),
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
            idempotent_streams: Mutex::new(HashMap::new()),
            persistence_hydration: TokioOnceCell::new(),
        })
    }

    pub async fn create_workspace(
        &self,
        request: CreateWorkspaceRequest,
    ) -> Result<WorkspaceCreateResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
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
        refs.sort_by_key(|entry| std::cmp::Reverse(entry.0));

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
        self.ensure_persistence_hydrated().await;
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
        self.persist_active_branch_pointer(&session_id, MAIN_BRANCH_ID)
            .await;
        if let Some(repo) = self.persistence_backend().await {
            if let Err(err) = repo
                .register_session_branch(&session_id, MAIN_BRANCH_ID)
                .await
            {
                warn!(
                    session_id = %session_id,
                    error = %err,
                    "failed to register main branch for new session in persistence backend"
                );
            }
        }
        self.persist_branch_state_value(&session_id, MAIN_BRANCH_ID, &SessionState::default())
            .await;

        Ok(SessionCreateResponse {
            session_id,
            created_at,
        })
    }

    pub async fn get_session_view(&self, session_id: &str) -> Option<SessionView> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        Some(SessionView {
            session_id: session.session_id.clone(),
            status: session.status.clone(),
            workflow_id: session.workflow_id.clone(),
            state_version: session.state.version,
        })
    }

    pub async fn handle_message_v1(
        &self,
        session_id: &str,
        request: SendMessageRequestV1,
    ) -> Result<SendMessageResponseV1, PipelineError> {
        self.ensure_persistence_hydrated().await;
        self.record_execution_step(
            session_id,
            "API_ENTRY",
            serde_json::json!({
                "route": "POST /api/v1/sessions/{sessionId}/messages",
            }),
        )
        .await?;

        if request.content.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "message content cannot be empty".to_owned(),
            ));
        }

        self.record_execution_step(session_id, "SESSION_HYDRATE_START", Value::Null)
            .await?;
        let lock_key = self.acquire_session_lock(session_id).await?;
        self.record_execution_step(
            session_id,
            "LOCK_ACQUIRED",
            serde_json::json!({ "lock_key": lock_key.clone() }),
        )
        .await?;

        let result = self.run_locked_message_pipeline(session_id, request).await;
        self.release_lock(&lock_key).await;
        let _ = self
            .record_execution_step(session_id, "LOCK_RELEASED", Value::Null)
            .await;
        result
    }

    async fn run_locked_message_pipeline(
        &self,
        session_id: &str,
        request: SendMessageRequestV1,
    ) -> Result<SendMessageResponseV1, PipelineError> {
        let context = self.hydrate_context(session_id).await;
        self.record_execution_step(
            session_id,
            "SESSION_HYDRATED",
            serde_json::json!({ "memory_len": context.memory.len() }),
        )
        .await?;

        let user_message = self
            .append_message(session_id, "user", &request.content)
            .await?;
        self.record_execution_step(
            session_id,
            "USER_MESSAGE_APPENDED",
            serde_json::json!({ "message_id": user_message.id }),
        )
        .await?;

        let (agent, reason) = select_agent(
            &request.content,
            &request.mode,
            context.forced_agent.as_ref(),
        );
        self.append_agent_log(session_id, &agent, &reason).await;
        self.record_execution_step(
            session_id,
            "AGENT_ROUTED",
            serde_json::json!({ "reason": reason }),
        )
        .await?;

        let agent_output = run_agent(agent, &request.content, &context, &request.mode);
        self.record_execution_step(session_id, "AGENT_EXECUTED", Value::Null)
            .await?;

        let prev_state = context.state.clone();
        let next_state = apply_state_mutation(&prev_state, &request.content, &agent_output);
        let ops = state_ops_from_states(&prev_state, &next_state);
        let diff = ops_to_diff_view(&ops);
        self.record_execution_step(
            session_id,
            "STATE_DIFF_COMPUTED",
            serde_json::json!({ "ops_count": ops.len() }),
        )
        .await?;

        self.append_event(session_id, EVENT_TYPE_STATE_UPDATE, diff.clone())
            .await?;

        let deltashot = self
            .create_deltashot(
                session_id,
                EVENT_TYPE_STATE_UPDATE,
                ops.clone(),
                next_state.clone(),
                DeltaShotMetadata {
                    event_type: EVENT_TYPE_STATE_UPDATE.to_owned(),
                    agent: Some(
                        match agent {
                            AgentKind::Claude => "claude",
                            AgentKind::DeepSeek => "deepseek",
                        }
                        .to_owned(),
                    ),
                    workflow_step: next_state.step.clone(),
                },
            )
            .await?;
        self.record_execution_step(
            session_id,
            "DELTASHOT_CREATED",
            serde_json::json!({ "deltashot_id": deltashot.id }),
        )
        .await?;

        let mut artifacts = Vec::new();
        if let (Some(artifact_type), Some(content)) = (
            agent_output.artifact_type.clone(),
            agent_output.artifact_content.clone(),
        ) {
            let artifact_outcome = self
                .create_or_update_artifact_internal(session_id, &artifact_type, &content)
                .await?;
            let artifact = artifact_outcome.envelope.clone();
            artifacts.push(artifact.clone());

            let content_diff = match artifact_outcome.previous_content {
                Some(previous) => serde_json::json!({
                    "from": previous,
                    "to": content,
                }),
                None => serde_json::json!({
                    "from": "",
                    "to": content,
                }),
            };

            let _ = self
                .append_event(
                    session_id,
                    EVENT_TYPE_ARTIFACT_UPDATE,
                    serde_json::json!({
                        "artifact_id": artifact.artifact_id,
                        "version": artifact.version,
                        "type": artifact.artifact_type,
                        "content_diff": content_diff,
                    }),
                )
                .await?;
            self.record_execution_step(
                session_id,
                "ARTIFACT_UPDATED",
                serde_json::json!({
                    "artifact_id": artifact.artifact_id,
                    "version": artifact.version
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
        self.record_execution_step(session_id, "AGENT_MESSAGE_APPENDED", Value::Null)
            .await?;

        self.persist_state(session_id, next_state.clone()).await?;
        self.record_execution_step(session_id, "STATE_PERSISTED", Value::Null)
            .await?;

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
            self.record_execution_step(
                session_id,
                "WORKFLOW_ENQUEUED",
                serde_json::json!({ "workflow_id": workflow_id, "step": workflow_step.clone() }),
            )
            .await?;
        }

        self.enqueue_notion_sync(session_id, &agent_output.text, &artifacts)
            .await;
        self.record_execution_step(session_id, "NOTION_SYNC_ENQUEUED", Value::Null)
            .await?;

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
                diff: ops_to_diff_value(&ds.ops),
                hash: ds.hash.clone(),
                prev_hash: if ds.prev_hash.is_empty() {
                    None
                } else {
                    Some(ds.prev_hash.clone())
                },
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
            diff: ops_to_diff_value(&ds.ops),
            hash: ds.hash.clone(),
            prev_hash: if ds.prev_hash.is_empty() {
                None
            } else {
                Some(ds.prev_hash.clone())
            },
        })
    }

    pub async fn rollback_session(
        &self,
        session_id: &str,
        request: RollbackRequest,
    ) -> Result<RollbackResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let lock_key = self.acquire_session_lock(session_id).await?;

        let result = async {
            let target_id = request.target_deltashot_id.clone();
            let mode = request.mode.to_ascii_lowercase();

            let (target_index, replay_ids, prev_state) = {
                let sessions = self.sessions.read().await;
                let session = sessions.get(session_id).ok_or_else(|| {
                    PipelineError::NotFound(format!("session '{}' not found", session_id))
                })?;
                let idx = session
                    .deltashot_ids
                    .iter()
                    .position(|id| id == &target_id)
                    .ok_or_else(|| {
                        PipelineError::NotFound(format!("deltashot '{}' not found", target_id))
                    })?;
                (
                    idx,
                    session.deltashot_ids[..=idx].to_vec(),
                    session.state.clone(),
                )
            };

            let rebuilt_state = self.replay_state_from_deltashot_ids(&replay_ids).await?;

            let mut sessions = self.sessions.write().await;
            let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
                SessionRecord::new(
                    session_id.to_owned(),
                    "adhoc".to_owned(),
                    "Adhoc Session".to_owned(),
                )
            });
            ensure_session_branches(session);
            session.state = rebuilt_state.clone();

            if mode == "hard" {
                session.deltashot_ids.truncate(target_index + 1);
                session.hashchain.truncate(target_index + 1);
            }
            sync_active_branch_from_session(session);

            drop(sessions);

            let rollback_ops = state_ops_from_states(&prev_state, &rebuilt_state);
            self.append_event(session_id, "ROLLBACK", ops_to_diff_value(&rollback_ops))
                .await?;

            let rollback_ds = self
                .create_deltashot(
                    session_id,
                    "ROLLBACK",
                    rollback_ops,
                    rebuilt_state.clone(),
                    DeltaShotMetadata {
                        event_type: "ROLLBACK".to_owned(),
                        agent: None,
                        workflow_step: None,
                    },
                )
                .await?;

            self.record_execution_step(
                session_id,
                "ROLLBACK_APPLIED",
                serde_json::json!({
                    "target_deltashot_id": target_id,
                    "mode": mode,
                    "new_deltashot_id": rollback_ds.id
                }),
            )
            .await?;

            Ok::<RollbackResponse, PipelineError>(RollbackResponse {
                status: "rolled_back".to_owned(),
                current_state_version: rebuilt_state.version,
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
        self.ensure_persistence_hydrated().await;
        let outcome = self
            .create_or_update_artifact_internal(
                &request.session_id,
                &request.artifact_type,
                &request.content,
            )
            .await?;
        Ok(outcome.envelope)
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
        self.ensure_persistence_hydrated().await;
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
        self.ensure_persistence_hydrated().await;
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

    pub async fn execute_next_workflow_job(
        &self,
    ) -> Result<Option<WorkflowExecutionResult>, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let next_job = {
            let mut queue = self.workflow_queue.lock().await;
            if queue.is_empty() {
                None
            } else {
                Some(queue.remove(0))
            }
        };

        let Some(job) = next_job else {
            return Ok(None);
        };

        let payload = serde_json::from_str::<Value>(&job.payload).unwrap_or(Value::Null);
        let content = format!(
            "workflow job {} step {} payload {}",
            job.workflow_id, job.step, payload
        );

        let request = SendMessageRequestV1 {
            content,
            mode: MessageMode::Workflow,
            metadata: serde_json::json!({
                "trigger": "workflow_queue",
                "workflow_id": job.workflow_id,
                "step": job.step,
                "enqueued_at": job.timestamp_ms,
            }),
        };

        match self.handle_message_v1(&job.session_id, request).await {
            Ok(response) => Ok(Some(WorkflowExecutionResult {
                workflow_id: job.workflow_id,
                session_id: job.session_id,
                step: job.step,
                deltashot_id: response.deltashot.id,
                state_version: response.state.version,
            })),
            Err(error) => {
                if matches!(error, PipelineError::LockUnavailable) {
                    let mut queue = self.workflow_queue.lock().await;
                    queue.push(job);
                }
                Err(error)
            }
        }
    }

    pub async fn force_agent_selection(
        &self,
        session_id: &str,
        request: ForceAgentRequest,
    ) -> Result<AgentSelectionView, PipelineError> {
        self.ensure_persistence_hydrated().await;
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

    pub async fn debug_audit_branch(
        &self,
        session_id: &str,
        branch_id: &str,
    ) -> Result<BranchAuditResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let redis_url = std::env::var("DELTA_AGENT_REDIS_URL").map_err(|_| {
            PipelineError::InvalidInput("DELTA_AGENT_REDIS_URL is not set".to_owned())
        })?;
        let vddab_root =
            std::env::var("DELTA_AGENT_VDDAB_ROOT").unwrap_or_else(|_| "./data/vddab".to_owned());

        let repo = RedisVddabRepository::connect(&redis_url, self.keyspace.clone(), vddab_root)
            .await
            .map_err(|err| PipelineError::Internal(format!("audit backend init failed: {err}")))?;
        let storage_branch_id = persistence_branch_key(session_id, branch_id);

        let chain = verify_chain(&repo, &storage_branch_id)
            .await
            .map_err(|err| PipelineError::Internal(format!("chain audit failed: {err}")))?;
        let replay = audit_replay(&repo, &repo, &storage_branch_id)
            .await
            .map_err(|err| PipelineError::Internal(format!("replay audit failed: {err}")))?;

        Ok(BranchAuditResponse {
            branch_id: branch_id.to_owned(),
            valid: chain.valid && replay.valid,
            chain,
            replay,
        })
    }

    async fn persistence_backend(&self) -> Option<RedisVddabRepository> {
        let config = self.persistence.as_ref()?;
        match RedisVddabRepository::connect(
            &config.redis_url,
            self.keyspace.clone(),
            &config.vddab_root,
        )
        .await
        {
            Ok(repo) => Some(repo),
            Err(err) => {
                warn!(error = %err, "failed to initialize persistence backend");
                None
            }
        }
    }

    async fn ensure_persistence_hydrated(&self) {
        if self.persistence.is_none() {
            return;
        }
        let _ = self
            .persistence_hydration
            .get_or_init(|| async {
                if let Err(err) = self.hydrate_from_persistence().await {
                    warn!(error = %err, "persistence hydration failed");
                }
            })
            .await;
    }

    async fn hydrate_from_persistence(&self) -> Result<(), PipelineError> {
        let Some(repo) = self.persistence_backend().await else {
            return Ok(());
        };

        let session_ids = repo.list_sessions().await.map_err(|err| {
            PipelineError::Internal(format!("failed to list persisted sessions: {err}"))
        })?;
        if session_ids.is_empty() {
            return Ok(());
        }

        let mut max_id = self.id_counter.load(Ordering::Relaxed);
        let mut recovered_deltashots = HashMap::<String, DeltashotRecord>::new();
        let mut recovered_sessions = HashMap::<String, SessionRecord>::new();

        for session_id in session_ids {
            update_max_id_seed(&mut max_id, &session_id);
            let mut branch_ids = repo
                .list_session_branches(&session_id)
                .await
                .map_err(|err| {
                    PipelineError::Internal(format!(
                        "failed to load persisted branches for session '{}': {err}",
                        session_id
                    ))
                })?;
            if branch_ids.is_empty() {
                branch_ids.push(MAIN_BRANCH_ID.to_owned());
            }
            if !branch_ids.iter().any(|branch| branch == MAIN_BRANCH_ID) {
                branch_ids.push(MAIN_BRANCH_ID.to_owned());
            }
            let active_branch_id = repo
                .get_session_active_branch(&session_id)
                .await
                .map_err(|err| {
                    PipelineError::Internal(format!(
                        "failed to load persisted active branch for session '{}': {err}",
                        session_id
                    ))
                })?
                .unwrap_or_else(|| MAIN_BRANCH_ID.to_owned());

            let mut session = SessionRecord::new(
                session_id.clone(),
                "rehydrated".to_owned(),
                "Rehydrated Session".to_owned(),
            );
            session.branches.clear();
            session.active_branch_id = active_branch_id.clone();

            for branch_id in branch_ids {
                update_max_id_seed(&mut max_id, &branch_id);
                let storage_branch_id = persistence_branch_key(&session_id, &branch_id);

                let deltas = repo
                    .load_branch_chain(&storage_branch_id)
                    .await
                    .map_err(|err| {
                        PipelineError::Internal(format!(
                            "failed to load persisted chain for '{}': {err}",
                            storage_branch_id
                        ))
                    })?;
                let mut replay_value = Value::Object(serde_json::Map::new());
                let mut hashchain = Vec::with_capacity(deltas.len());
                let mut deltashot_ids = Vec::with_capacity(deltas.len());
                for ds in deltas {
                    update_max_id_seed(&mut max_id, &ds.id);
                    replay_value = apply_ops_to_state(&replay_value, &ds.ops)
                        .map_err(|err| PipelineError::Internal(err.to_string()))?;
                    let state_snapshot = state_from_value(replay_value.clone())?;
                    recovered_deltashots.insert(
                        ds.id.clone(),
                        DeltashotRecord {
                            id: ds.id.clone(),
                            session_id: session_id.clone(),
                            branch_id: branch_id.clone(),
                            timestamp: ds.timestamp as u64,
                            event_type: ds.metadata.event_type.clone(),
                            ops: ds.ops.clone(),
                            hash: ds.hash.clone(),
                            prev_hash: ds.prev_hash.clone(),
                            state_snapshot,
                            metadata: ds.metadata.clone(),
                        },
                    );
                    hashchain.push(ds.hash);
                    deltashot_ids.push(ds.id);
                }

                let persisted_state = repo
                    .get_branch_state(&storage_branch_id)
                    .await
                    .unwrap_or_else(|_| replay_value.clone());
                let branch_state = state_from_value(persisted_state)
                    .unwrap_or_else(|_| state_from_value(replay_value.clone()).unwrap_or_default());
                let branch = BranchRecord {
                    branch_id: branch_id.clone(),
                    _session_id: session_id.clone(),
                    _parent_deltashot_id: deltashot_ids.first().cloned().unwrap_or_default(),
                    _created_at: now_ms(),
                    label: if branch_id == MAIN_BRANCH_ID {
                        None
                    } else {
                        Some("rehydrated".to_owned())
                    },
                    mode: BranchMode::Soft,
                    state: branch_state,
                    messages: Vec::new(),
                    events: Vec::new(),
                    hashchain,
                    deltashot_ids,
                    artifacts: HashSet::new(),
                };
                session.branches.insert(branch_id, branch);
            }

            if !session.branches.contains_key(&session.active_branch_id) {
                session.active_branch_id = MAIN_BRANCH_ID.to_owned();
            }
            let active_branch_id = session.active_branch_id.clone();
            if load_branch_into_session(&mut session, &active_branch_id).is_err() {
                session.active_branch_id = MAIN_BRANCH_ID.to_owned();
                let _ = load_branch_into_session(&mut session, MAIN_BRANCH_ID);
            }
            recovered_sessions.insert(session_id, session);
        }

        {
            let mut sessions = self.sessions.write().await;
            for (session_id, record) in recovered_sessions {
                sessions.entry(session_id).or_insert(record);
            }
        }
        {
            let mut deltas = self.deltashots.write().await;
            for (id, record) in recovered_deltashots {
                deltas.entry(id).or_insert(record);
            }
        }
        seed_counter(&self.id_counter, max_id.saturating_add(1));
        Ok(())
    }

    async fn branch_deltashot_count(&self, session_id: &str, branch_id: &str) -> Option<usize> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        let branch = session.branches.get(branch_id)?;
        Some(branch.deltashot_ids.len())
    }

    async fn persist_active_branch_pointer(&self, session_id: &str, branch_id: &str) {
        if let Some(repo) = self.persistence_backend().await {
            if let Err(err) = repo.set_session_active_branch(session_id, branch_id).await {
                warn!(
                    session_id = %session_id,
                    branch_id = %branch_id,
                    error = %err,
                    "failed to persist active branch pointer"
                );
            }
        }
    }

    async fn persist_branch_state_value(
        &self,
        session_id: &str,
        branch_id: &str,
        state: &SessionState,
    ) {
        if let Some(repo) = self.persistence_backend().await {
            let storage_branch_id = persistence_branch_key(session_id, branch_id);
            let value = value_from_state(state);
            if let Err(err) = repo.store_branch_state(&storage_branch_id, &value).await {
                warn!(
                    branch_id = %branch_id,
                    storage_branch_id = %storage_branch_id,
                    error = %err,
                    "failed to persist branch state"
                );
            }
        }
    }

    async fn persist_deltashot_record(&self, record: &DeltashotRecord) {
        if let Some(repo) = self.persistence_backend().await {
            let storage_branch_id = persistence_branch_key(&record.session_id, &record.branch_id);
            let delta = DeltaShot {
                id: record.id.clone(),
                session_id: record.session_id.clone(),
                branch_id: record.branch_id.clone(),
                prev_hash: record.prev_hash.clone(),
                hash: record.hash.clone(),
                timestamp: u128::from(record.timestamp),
                ops: record.ops.clone(),
                artifacts: Vec::new(),
                metadata: record.metadata.clone(),
            };
            let ops_json = match serde_json::to_vec(&record.ops) {
                Ok(value) => value,
                Err(err) => {
                    warn!(deltashot_id = %record.id, error = %err, "failed to serialize ops");
                    return;
                }
            };
            let compressed = compress_ops(&ops_json);
            if let Err(err) = repo
                .store_deltashot(&delta, &storage_branch_id, &compressed)
                .await
            {
                warn!(
                    deltashot_id = %record.id,
                    branch_id = %record.branch_id,
                    storage_branch_id = %storage_branch_id,
                    error = %err,
                    "failed to persist deltashot"
                );
                return;
            }
            let state_value = value_from_state(&record.state_snapshot);
            if let Err(err) = repo
                .store_branch_state(&storage_branch_id, &state_value)
                .await
            {
                warn!(
                    branch_id = %record.branch_id,
                    storage_branch_id = %storage_branch_id,
                    error = %err,
                    "failed to persist branch state after deltashot"
                );
            }
            let snapshot_index = self
                .branch_deltashot_count(&record.session_id, &record.branch_id)
                .await
                .unwrap_or(0);
            if snapshot_index > 0
                && snapshot_index.is_multiple_of(SNAPSHOT_INTERVAL_EVENTS as usize)
            {
                if let Err(err) = repo
                    .store_snapshot(&storage_branch_id, snapshot_index, &state_value)
                    .await
                {
                    warn!(
                        branch_id = %record.branch_id,
                        storage_branch_id = %storage_branch_id,
                        snapshot_index,
                        error = %err,
                        "failed to persist snapshot after deltashot"
                    );
                }
            }
        }
    }

    pub async fn create_branch(
        &self,
        session_id: &str,
        request: BranchCreateRequest,
    ) -> Result<BranchCreateResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        if request.label.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "branch label cannot be empty".to_owned(),
            ));
        }

        let (source_branch, source_idx) = {
            let mut sessions = self.sessions.write().await;
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            })?;
            ensure_session_branches(session);
            let mut source = None;
            for branch in session.branches.values() {
                if let Some(idx) = branch
                    .deltashot_ids
                    .iter()
                    .position(|id| id == &request.from_deltashot_id)
                {
                    source = Some((branch.clone(), idx));
                    break;
                }
            }
            source.ok_or_else(|| {
                PipelineError::NotFound(format!(
                    "deltashot '{}' not found in any branch",
                    request.from_deltashot_id
                ))
            })?
        };

        let parent_snapshot = {
            let map = self.deltashots.read().await;
            map.get(&request.from_deltashot_id)
                .map(|record| (record.state_snapshot.clone(), record.timestamp))
                .ok_or_else(|| {
                    PipelineError::NotFound(format!(
                        "deltashot '{}' not found",
                        request.from_deltashot_id
                    ))
                })?
        };

        let branch_id = self.next_id("br");
        let mut branch_artifacts = source_branch.artifacts.clone();
        if matches!(request.mode, BranchMode::Hard) {
            let mut artifacts = self.artifacts.write().await;
            let mut duplicated = HashSet::new();
            for source_artifact_id in &source_branch.artifacts {
                let Some(record) = artifacts.get(source_artifact_id).cloned() else {
                    continue;
                };
                let duplicated_id = self.next_id("art");
                let mut cloned = record.clone();
                cloned.artifact_id = duplicated_id.clone();
                cloned.branch_id = branch_id.clone();
                cloned.session_id = session_id.to_owned();
                artifacts.insert(duplicated_id.clone(), cloned);
                duplicated.insert(duplicated_id);
            }
            branch_artifacts = duplicated;
        }

        let hashchain_cutoff = source_idx
            .saturating_add(1)
            .min(source_branch.hashchain.len());
        let inherited_deltashot_ids = source_branch.deltashot_ids[..=source_idx].to_vec();
        let parent_state = parent_snapshot.0;
        let parent_timestamp = parent_snapshot.1;
        let new_branch = BranchRecord {
            branch_id: branch_id.clone(),
            _session_id: session_id.to_owned(),
            _parent_deltashot_id: request.from_deltashot_id.clone(),
            _created_at: now_ms(),
            label: Some(request.label),
            mode: request.mode.clone(),
            state: parent_state.clone(),
            messages: source_branch
                .messages
                .iter()
                .filter(|message| message.timestamp <= parent_timestamp)
                .cloned()
                .collect::<Vec<_>>(),
            events: source_branch
                .events
                .iter()
                .filter(|event| event.timestamp <= parent_timestamp)
                .cloned()
                .collect::<Vec<_>>(),
            hashchain: source_branch.hashchain[..hashchain_cutoff].to_vec(),
            deltashot_ids: inherited_deltashot_ids.clone(),
            artifacts: branch_artifacts,
        };

        let active_branch_after_create = {
            let mut sessions = self.sessions.write().await;
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            })?;
            ensure_session_branches(session);
            session.branches.insert(branch_id.clone(), new_branch);
            session.active_branch_id.clone()
        };
        self.persist_active_branch_pointer(session_id, &active_branch_after_create)
            .await;
        if let Some(repo) = self.persistence_backend().await {
            if let Err(err) = repo.register_session_branch(session_id, &branch_id).await {
                warn!(
                    session_id = %session_id,
                    branch_id = %branch_id,
                    error = %err,
                    "failed to register created branch in persistence backend"
                );
            }
            let storage_branch_id = persistence_branch_key(session_id, &branch_id);
            if let Err(err) = repo
                .replace_branch_chain(&storage_branch_id, &inherited_deltashot_ids)
                .await
            {
                warn!(
                    session_id = %session_id,
                    branch_id = %branch_id,
                    storage_branch_id = %storage_branch_id,
                    error = %err,
                    "failed to seed created branch chain in persistence backend"
                );
            }
        }
        self.persist_branch_state_value(session_id, &branch_id, &parent_state)
            .await;

        Ok(BranchCreateResponse {
            branch: BranchPointer {
                branch_id,
                parent_deltashot_id: request.from_deltashot_id,
                created_at: now_ms(),
            },
            state: BranchStateView {
                version: parent_state.version,
                forked: true,
            },
        })
    }

    pub async fn list_branches(
        &self,
        session_id: &str,
    ) -> Result<BranchListResponse, PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        ensure_session_branches(session);

        let mut branches = session
            .branches
            .values()
            .map(|branch| BranchSummary {
                branch_id: branch.branch_id.clone(),
                is_main: branch.branch_id == MAIN_BRANCH_ID,
                label: branch.label.clone(),
            })
            .collect::<Vec<_>>();

        branches.sort_by(|a, b| a.branch_id.cmp(&b.branch_id));
        branches.sort_by_key(|entry| std::cmp::Reverse(entry.is_main));

        Ok(BranchListResponse { branches })
    }

    pub async fn switch_branch(
        &self,
        session_id: &str,
        request: BranchSwitchRequest,
    ) -> Result<BranchSwitchResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let branch_id = request.branch_id.clone();
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        ensure_session_branches(session);
        load_branch_into_session(session, &request.branch_id)?;
        drop(sessions);
        self.persist_active_branch_pointer(session_id, &branch_id)
            .await;

        Ok(BranchSwitchResponse {
            branch_id: request.branch_id,
            status: "switched".to_owned(),
        })
    }

    pub async fn merge_branch(
        &self,
        session_id: &str,
        request: BranchMergeRequest,
    ) -> Result<BranchMergeResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let strategy = request.strategy.to_ascii_lowercase();
        let source_branch_id = request.source_branch.clone();
        let target_branch_id = request.target_branch.clone();
        if strategy != "fast-forward" && strategy != "rebase" && strategy != "manual" {
            return Err(PipelineError::InvalidInput(
                "strategy must be one of: fast-forward | rebase | manual".to_owned(),
            ));
        }

        let merge_record = {
            let mut sessions = self.sessions.write().await;
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            })?;
            ensure_session_branches(session);

            let source = session
                .branches
                .get(&source_branch_id)
                .cloned()
                .ok_or_else(|| {
                    PipelineError::NotFound(format!(
                        "source branch '{}' not found",
                        source_branch_id
                    ))
                })?;
            let target = session
                .branches
                .get(&target_branch_id)
                .cloned()
                .ok_or_else(|| {
                    PipelineError::NotFound(format!(
                        "target branch '{}' not found",
                        target_branch_id
                    ))
                })?;

            let merge_ops = state_ops_from_states(&target.state, &source.state);
            let new_deltashot_id = self.next_id("ds_merge");
            let prev_hash = target.hashchain.last().cloned().unwrap_or_default();
            let canonical = canonical_serialize_ops(&merge_ops)
                .map_err(|err| PipelineError::Serialization(err.to_string()))?;
            let hash = compute_chain_hash(&prev_hash, &canonical);

            let merge_event = SessionEvent {
                id: self.next_id("evt"),
                event_type: "BRANCH_MERGE".to_owned(),
                payload: serde_json::json!({
                    "source_branch": source_branch_id,
                    "target_branch": target_branch_id,
                    "strategy": strategy.clone(),
                }),
                timestamp: now_ms(),
            };

            let mut merged_target = target;
            merged_target.mode = BranchMode::Soft;
            merged_target.state = source.state.clone();
            merged_target.messages = source.messages.clone();
            merged_target.events.push(merge_event);
            merged_target.hashchain.push(hash.clone());
            merged_target.deltashot_ids.push(new_deltashot_id.clone());
            merged_target.artifacts = source.artifacts.clone();
            session
                .branches
                .insert(target_branch_id.clone(), merged_target);

            if session.active_branch_id == target_branch_id {
                load_branch_into_session(session, &target_branch_id)?;
            }

            DeltashotRecord {
                id: new_deltashot_id,
                session_id: session_id.to_owned(),
                branch_id: target_branch_id,
                timestamp: now_ms(),
                event_type: "BRANCH_MERGE".to_owned(),
                ops: merge_ops,
                hash,
                prev_hash,
                state_snapshot: source.state,
                metadata: DeltaShotMetadata {
                    event_type: "BRANCH_MERGE".to_owned(),
                    agent: None,
                    workflow_step: None,
                },
            }
        };

        {
            let mut deltashots = self.deltashots.write().await;
            deltashots.insert(merge_record.id.clone(), merge_record.clone());
        }
        self.persist_deltashot_record(&merge_record).await;

        Ok(BranchMergeResponse {
            status: "merged".to_owned(),
            new_deltashot_id: merge_record.id,
        })
    }

    pub async fn stream_message_v1(
        &self,
        session_id: &str,
        request: StreamMessageRequestV1,
        idempotency_key: Option<String>,
    ) -> Vec<StreamEventEnvelope> {
        self.ensure_persistence_hydrated().await;
        let cache_key = idempotency_key
            .as_ref()
            .map(|key| format!("{session_id}:{key}"));
        if let Some(key) = cache_key.as_ref() {
            let cache = self.idempotent_streams.lock().await;
            if let Some(cached) = cache.get(key) {
                return cached.clone();
            }
        }

        let request_id = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("req_{}", uuid::Uuid::new_v4().simple()));
        let mut events = vec![stream_event("ack", StreamAckEvent { request_id })];

        if request.content.trim().is_empty() {
            events.push(stream_event(
                "error",
                StreamErrorEvent {
                    code: PipelineError::InvalidInput("message content cannot be empty".to_owned())
                        .code()
                        .to_owned(),
                    retryable: false,
                },
            ));
            if let Some(key) = cache_key {
                let mut cache = self.idempotent_streams.lock().await;
                cache.insert(key, events.clone());
            }
            return events;
        }

        if let Some(branch_id) = request.branch.clone() {
            if let Err(error) = self
                .switch_branch(session_id, BranchSwitchRequest { branch_id })
                .await
            {
                events.push(stream_event(
                    "error",
                    StreamErrorEvent {
                        code: error.code().to_owned(),
                        retryable: error.retryable(),
                    },
                ));
                if let Some(key) = cache_key {
                    let mut cache = self.idempotent_streams.lock().await;
                    cache.insert(key, events.clone());
                }
                return events;
            }
        }

        let lock_key = match self.acquire_session_lock(session_id).await {
            Ok(key) => key,
            Err(error) => {
                events.push(stream_event(
                    "error",
                    StreamErrorEvent {
                        code: error.code().to_owned(),
                        retryable: error.retryable(),
                    },
                ));
                if let Some(key) = cache_key {
                    let mut cache = self.idempotent_streams.lock().await;
                    cache.insert(key, events.clone());
                }
                return events;
            }
        };

        let result = self
            .run_locked_stream_pipeline(session_id, request, &mut events)
            .await;
        self.release_lock(&lock_key).await;

        match result {
            Ok(_) => events.push(stream_event(
                "done",
                StreamDoneEvent {
                    status: "complete".to_owned(),
                },
            )),
            Err(error) => events.push(stream_event(
                "error",
                StreamErrorEvent {
                    code: error.code().to_owned(),
                    retryable: error.retryable(),
                },
            )),
        }

        if let Some(key) = cache_key {
            let mut cache = self.idempotent_streams.lock().await;
            cache.insert(key, events.clone());
        }
        events
    }

    async fn run_locked_stream_pipeline(
        &self,
        session_id: &str,
        request: StreamMessageRequestV1,
        events: &mut Vec<StreamEventEnvelope>,
    ) -> Result<(), PipelineError> {
        let context = self.hydrate_context(session_id).await;
        let _ = self
            .append_message(session_id, "user", &request.content)
            .await?;

        let (agent, reason) = select_agent(
            &request.content,
            &request.mode,
            context.forced_agent.as_ref(),
        );
        self.append_agent_log(session_id, &agent, &reason).await;

        let agent_output = run_agent(agent, &request.content, &context, &request.mode);
        let mut accumulated = String::new();
        for token in tokenize_stream_chunks(&agent_output.text) {
            if !accumulated.is_empty() {
                accumulated.push(' ');
            }
            accumulated.push_str(&token);
            events.push(stream_event(
                "token",
                StreamTokenEvent {
                    delta: token,
                    accumulated: accumulated.clone(),
                },
            ));
        }

        let prev_state = context.state.clone();
        let next_state = apply_state_mutation(&prev_state, &request.content, &agent_output);
        let ops = state_ops_from_states(&prev_state, &next_state);
        let diff = ops_to_diff_view(&ops);
        self.append_event(session_id, EVENT_TYPE_STATE_UPDATE, diff.clone())
            .await?;
        let deltashot = self
            .create_deltashot(
                session_id,
                EVENT_TYPE_STATE_UPDATE,
                ops,
                next_state.clone(),
                DeltaShotMetadata {
                    event_type: EVENT_TYPE_STATE_UPDATE.to_owned(),
                    agent: Some(
                        match agent {
                            AgentKind::Claude => "claude",
                            AgentKind::DeepSeek => "deepseek",
                        }
                        .to_owned(),
                    ),
                    workflow_step: next_state.step.clone(),
                },
            )
            .await?;

        let mut artifact_envelopes = Vec::new();
        if let (Some(artifact_type), Some(content)) = (
            agent_output.artifact_type.clone(),
            agent_output.artifact_content.clone(),
        ) {
            let outcome = self
                .create_or_update_artifact_internal(session_id, &artifact_type, &content)
                .await?;
            artifact_envelopes.push(outcome.envelope.clone());
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

        events.push(stream_event(
            "message",
            StreamMessageEvent {
                message_id: agent_message.id,
                content: agent_output.text.clone(),
            },
        ));
        events.push(stream_event(
            "deltashot",
            StreamDeltaShotEvent {
                id: deltashot.id,
                event_type: EVENT_TYPE_STATE_UPDATE.to_owned(),
            },
        ));
        for artifact in artifact_envelopes {
            events.push(stream_event(
                "artifact",
                StreamArtifactEvent {
                    artifact_id: artifact.artifact_id,
                    version: artifact.version,
                    artifact_type: artifact.artifact_type,
                },
            ));
        }

        if matches!(request.mode, MessageMode::Workflow | MessageMode::Execution) {
            let workflow_step = next_state.step.unwrap_or_else(|| "continue".to_owned());
            events.push(stream_event(
                "workflow",
                StreamWorkflowEvent {
                    step: workflow_step,
                    status: "started".to_owned(),
                },
            ));
        }

        Ok(())
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
        ensure_session_branches(session);

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
        ensure_session_branches(session);
        session.messages.push(message.clone());
        sync_active_branch_from_session(session);

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
        ensure_session_branches(session);
        session.events.push(event.clone());
        sync_active_branch_from_session(session);
        Ok(event)
    }

    async fn create_deltashot(
        &self,
        session_id: &str,
        event_type: &str,
        ops: Vec<DeltaShotOp>,
        state_snapshot: SessionState,
        metadata: DeltaShotMetadata,
    ) -> Result<DeltashotRecord, PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
            SessionRecord::new(
                session_id.to_owned(),
                "adhoc".to_owned(),
                "Adhoc Session".to_owned(),
            )
        });
        ensure_session_branches(session);

        let id = self.next_id("ds");
        let timestamp = now_ms();
        let branch_id = session.active_branch_id.clone();
        let prev_hash = session.hashchain.last().cloned().unwrap_or_default();
        let canonical = canonical_serialize_ops(&ops)
            .map_err(|err| PipelineError::Serialization(err.to_string()))?;
        let hash = compute_chain_hash(&prev_hash, &canonical);

        let record = DeltashotRecord {
            id: id.clone(),
            session_id: session_id.to_owned(),
            branch_id,
            timestamp,
            event_type: event_type.to_owned(),
            ops,
            hash: hash.clone(),
            prev_hash,
            state_snapshot,
            metadata,
        };

        session.hashchain.push(hash);
        session.deltashot_ids.push(id.clone());
        sync_active_branch_from_session(session);
        drop(sessions);

        let mut all = self.deltashots.write().await;
        all.insert(id, record.clone());
        drop(all);
        self.persist_deltashot_record(&record).await;
        Ok(record)
    }

    async fn create_or_update_artifact_internal(
        &self,
        session_id: &str,
        artifact_type: &str,
        content: &str,
    ) -> Result<ArtifactWriteOutcome, PipelineError> {
        let (active_branch_id, branch_artifact_ids) = {
            let mut sessions = self.sessions.write().await;
            let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
                SessionRecord::new(
                    session_id.to_owned(),
                    "adhoc".to_owned(),
                    "Adhoc Session".to_owned(),
                )
            });
            ensure_session_branches(session);
            (session.active_branch_id.clone(), session.artifacts.clone())
        };

        let mut artifacts = self.artifacts.write().await;
        let matching_artifact_id = branch_artifact_ids.iter().find_map(|artifact_id| {
            artifacts.get(artifact_id).and_then(|record| {
                (record.artifact_type == artifact_type).then_some(artifact_id.clone())
            })
        });

        let mut replaced_artifact_id = None;
        let (artifact_id, artifact_type, version, previous_content) = match matching_artifact_id {
            Some(existing_id) => {
                let existing = artifacts.get(&existing_id).cloned().ok_or_else(|| {
                    PipelineError::NotFound(format!("artifact '{}' not found", existing_id))
                })?;
                if existing.branch_id == active_branch_id {
                    let record = artifacts.get_mut(&existing_id).ok_or_else(|| {
                        PipelineError::NotFound(format!("artifact '{}' not found", existing_id))
                    })?;
                    let previous = record.versions.get(&record.current_version).cloned();
                    record.current_version += 1;
                    record
                        .versions
                        .insert(record.current_version, content.to_owned());
                    (
                        existing_id,
                        record.artifact_type.clone(),
                        record.current_version,
                        previous,
                    )
                } else {
                    let new_id = self.next_id("art");
                    let mut cloned = existing.clone();
                    cloned.artifact_id = new_id.clone();
                    cloned.branch_id = active_branch_id.clone();
                    cloned.current_version += 1;
                    let previous = existing.versions.get(&existing.current_version).cloned();
                    cloned
                        .versions
                        .insert(cloned.current_version, content.to_owned());
                    artifacts.insert(new_id.clone(), cloned.clone());
                    replaced_artifact_id = Some(existing_id);
                    (
                        new_id,
                        cloned.artifact_type,
                        cloned.current_version,
                        previous,
                    )
                }
            }
            None => {
                let new_id = self.next_id("art");
                let mut versions = BTreeMap::new();
                versions.insert(1, content.to_owned());
                artifacts.insert(
                    new_id.clone(),
                    ArtifactRecord {
                        artifact_id: new_id.clone(),
                        session_id: session_id.to_owned(),
                        branch_id: active_branch_id.clone(),
                        artifact_type: artifact_type.to_owned(),
                        created_at: now_ms(),
                        current_version: 1,
                        versions,
                    },
                );
                (new_id, artifact_type.to_owned(), 1, None)
            }
        };
        drop(artifacts);

        {
            let mut sessions = self.sessions.write().await;
            let session = sessions.entry(session_id.to_owned()).or_insert_with(|| {
                SessionRecord::new(
                    session_id.to_owned(),
                    "adhoc".to_owned(),
                    "Adhoc Session".to_owned(),
                )
            });
            ensure_session_branches(session);
            if let Some(previous_id) = replaced_artifact_id {
                session.artifacts.remove(&previous_id);
            }
            session.artifacts.insert(artifact_id.clone());
            sync_active_branch_from_session(session);
        }

        Ok(ArtifactWriteOutcome {
            envelope: ArtifactEnvelope {
                artifact_id,
                version,
                artifact_type,
            },
            previous_content,
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
        ensure_session_branches(session);
        session.state = next_state;
        sync_active_branch_from_session(session);
        let branch_id = session.active_branch_id.clone();
        let state = session.state.clone();
        drop(sessions);
        self.persist_branch_state_value(session_id, &branch_id, &state)
            .await;
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
            info!(
                session_id = %job.session_id,
                summary = %job.summary,
                artifacts_count = job.artifacts.len(),
                "notion sync job recorded"
            );
            q.push(job);
            info!(session_id = %session_id, "notion sync enqueued");
        });
    }

    async fn record_execution_step(
        &self,
        session_id: &str,
        step: &str,
        details: Value,
    ) -> Result<(), PipelineError> {
        let _ = self
            .append_event(
                session_id,
                EVENT_TYPE_EXECUTION_STEP,
                serde_json::json!({
                    "step": step,
                    "details": details,
                }),
            )
            .await?;
        Ok(())
    }

    async fn replay_state_from_deltashot_ids(
        &self,
        deltashot_ids: &[String],
    ) -> Result<SessionState, PipelineError> {
        let map = self.deltashots.read().await;
        let mut rebuilt = SessionState::default();
        for id in deltashot_ids {
            let ds = map.get(id).ok_or_else(|| {
                PipelineError::NotFound(format!("deltashot '{}' not found during replay", id))
            })?;
            if ds.event_type == EVENT_TYPE_STATE_UPDATE || ds.event_type == "ROLLBACK" {
                let value = value_from_state(&rebuilt);
                let updated = apply_ops_to_state(&value, &ds.ops)
                    .map_err(|err| PipelineError::Internal(err.to_string()))?;
                rebuilt = state_from_value(updated)?;
            }
        }
        Ok(rebuilt)
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

// ==========================================================
// Section 4: Runtime entrypoints + deterministic helpers
// ==========================================================

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
        let rendered = render_artifact_content("code", input);
        (Some("code".to_owned()), Some(rendered))
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

fn render_artifact_content(artifact_type: &str, source: &str) -> String {
    format!(
        "{{\"type\":\"{artifact_type}\",\"rendered_from\":\"{source}\",\"engine\":\"deterministic\"}}"
    )
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

fn state_ops_from_states(prev: &SessionState, next: &SessionState) -> Vec<DeltaShotOp> {
    let prev_value = value_from_state(prev);
    let next_value = value_from_state(next);
    compute_diff_ops(&prev_value, &next_value).unwrap_or_default()
}

fn ops_to_diff_value(ops: &[DeltaShotOp]) -> Value {
    let mut set_count = 0usize;
    let mut replace_count = 0usize;
    let mut append_count = 0usize;
    let mut delete_count = 0usize;
    for op in ops {
        match op.op_type {
            OpType::Set => set_count += 1,
            OpType::Replace => replace_count += 1,
            OpType::Append => append_count += 1,
            OpType::Delete => delete_count += 1,
        }
    }
    serde_json::json!({
        "type": EVENT_TYPE_STATE_UPDATE,
        "ops": ops,
        "summary": {
            "set": set_count,
            "replace": replace_count,
            "append": append_count,
            "delete": delete_count
        }
    })
}

fn ops_to_diff_view(ops: &[DeltaShotOp]) -> Value {
    ops_to_diff_value(ops)
}

fn value_from_state(state: &SessionState) -> Value {
    serde_json::json!({
        "goal": state.goal,
        "step": state.step,
        "last_user_message": state.last_user_message,
        "last_agent_message": state.last_agent_message,
        "version": state.version,
    })
}

fn state_from_value(value: Value) -> Result<SessionState, PipelineError> {
    let object = value.as_object().ok_or_else(|| {
        PipelineError::Serialization("state replay payload was not an object".to_owned())
    })?;

    let goal = object
        .get("goal")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let step = object
        .get("step")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let last_user_message = object
        .get("last_user_message")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let last_agent_message = object
        .get("last_agent_message")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let version = object.get("version").and_then(Value::as_u64).unwrap_or(0);

    Ok(SessionState {
        goal,
        step,
        last_user_message,
        last_agent_message,
        version,
    })
}

fn ensure_session_branches(session: &mut SessionRecord) {
    if !session.branches.contains_key(MAIN_BRANCH_ID) {
        let main = BranchRecord {
            branch_id: MAIN_BRANCH_ID.to_owned(),
            _session_id: session.session_id.clone(),
            _parent_deltashot_id: session.deltashot_ids.first().cloned().unwrap_or_default(),
            _created_at: session.created_at,
            label: None,
            mode: BranchMode::Soft,
            state: session.state.clone(),
            messages: session.messages.clone(),
            events: session.events.clone(),
            hashchain: session.hashchain.clone(),
            deltashot_ids: session.deltashot_ids.clone(),
            artifacts: session.artifacts.clone(),
        };
        session.branches.insert(MAIN_BRANCH_ID.to_owned(), main);
    }

    if !session.branches.contains_key(&session.active_branch_id) {
        session.active_branch_id = MAIN_BRANCH_ID.to_owned();
    }
}

fn active_branch_mut(session: &mut SessionRecord) -> Result<&mut BranchRecord, PipelineError> {
    ensure_session_branches(session);
    session
        .branches
        .get_mut(&session.active_branch_id)
        .ok_or_else(|| PipelineError::Internal("active branch missing".to_owned()))
}

fn sync_active_branch_from_session(session: &mut SessionRecord) {
    let snapshot_state = session.state.clone();
    let snapshot_messages = session.messages.clone();
    let snapshot_events = session.events.clone();
    let snapshot_hashchain = session.hashchain.clone();
    let snapshot_deltashots = session.deltashot_ids.clone();
    let snapshot_artifacts = session.artifacts.clone();
    if let Ok(branch) = active_branch_mut(session) {
        branch.state = snapshot_state;
        branch.messages = snapshot_messages;
        branch.events = snapshot_events;
        branch.hashchain = snapshot_hashchain;
        branch.deltashot_ids = snapshot_deltashots;
        branch.artifacts = snapshot_artifacts;
    }
}

fn load_branch_into_session(
    session: &mut SessionRecord,
    branch_id: &str,
) -> Result<(), PipelineError> {
    ensure_session_branches(session);
    let branch = session
        .branches
        .get(branch_id)
        .cloned()
        .ok_or_else(|| PipelineError::NotFound(format!("branch '{}' not found", branch_id)))?;
    session.active_branch_id = branch.branch_id.clone();
    session.state = branch.state;
    session.messages = branch.messages;
    session.events = branch.events;
    session.hashchain = branch.hashchain;
    session.deltashot_ids = branch.deltashot_ids;
    session.artifacts = branch.artifacts;
    Ok(())
}

fn tokenize_stream_chunks(content: &str) -> Vec<String> {
    content
        .split_whitespace()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
}

fn stream_event<T: Serialize>(event: &str, data: T) -> StreamEventEnvelope {
    let payload = serde_json::to_value(data).unwrap_or(Value::Null);
    StreamEventEnvelope {
        event: event.to_owned(),
        data: payload,
    }
}

fn persistence_branch_key(session_id: &str, branch_id: &str) -> String {
    format!(
        "s{}_{}_b{}_{}",
        session_id.len(),
        session_id,
        branch_id.len(),
        branch_id
    )
}

fn update_max_id_seed(seed: &mut u64, id: &str) {
    if let Some((_, suffix)) = id.rsplit_once('_') {
        if let Ok(parsed) = suffix.parse::<u64>() {
            *seed = (*seed).max(parsed);
        }
    }
}

fn seed_counter(counter: &AtomicU64, candidate_next: u64) {
    loop {
        let current = counter.load(Ordering::Relaxed);
        if current >= candidate_next {
            return;
        }
        if counter
            .compare_exchange(
                current,
                candidate_next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            return;
        }
    }
}

fn load_persistence_config() -> Option<PersistenceConfig> {
    let redis_url = std::env::var("DELTA_AGENT_REDIS_URL").ok()?;
    if redis_url.trim().is_empty() {
        return None;
    }
    let vddab_root =
        std::env::var("DELTA_AGENT_VDDAB_ROOT").unwrap_or_else(|_| "./data/vddab".to_owned());
    Some(PersistenceConfig {
        redis_url,
        vddab_root,
    })
}

fn compress_ops(payload: &[u8]) -> Vec<u8> {
    let mut compressed = Vec::new();
    let mut reader = brotli::CompressorReader::new(payload, 4096, 5, 20);
    if reader.read_to_end(&mut compressed).is_ok() {
        compressed
    } else {
        payload.to_vec()
    }
}

// ==========================================================
// Section 5: Tests
// ==========================================================

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
