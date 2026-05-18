use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use delta_core::redis_schema::{
    validate_spec_integrity, RedisKeyspace, EVENT_TYPE_ARTIFACT_UPDATE, EVENT_TYPE_MESSAGE_APPEND,
    EVENT_TYPE_STATE_UPDATE, SNAPSHOT_INTERVAL_EVENTS,
};
use deltashot::{
    apply_ops_to_state, canonical_serialize_ops, compute_chain_hash, compute_diff_ops, DeltaShot,
    DeltaShotMetadata, DeltaShotOp, OpType,
};
use futures_util::StreamExt;
use replay::{
    adapters::{
        DurableNotionSyncJob, DurableWorkflowJob, PersistedAgentLogEntry, PersistedArtifactRecord,
        PersistedArtifactVersion, PersistedSessionEvent, PersistedSessionMessage,
        PersistedSessionMeta, PersistedWorkflowRecord, PersistedWorkspaceRecord,
        RedisVddabRepository,
    },
    verifier::{audit_replay, verify_chain, ReplayAuditResult, VerificationResult},
    verifier::{DeltaRepository, StateRepository},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell as TokioOnceCell, RwLock};
use tracing::{info, warn};

use crate::agents::config::AgentRuntimeConfig;
use crate::agents::errors::AgentError;
use crate::agents::router::RealAgentRouter;
use crate::agents::types::{
    AgentMessage as ClientAgentMessage, AgentMode as ClientAgentMode, AgentProvider, AgentRequest,
};

const RECENT_MESSAGE_WINDOW: usize = 20;
const SESSION_LOCK_TTL_SECS: u64 = 5;
const SESSION_LOCK_RETRIES: usize = 3;
const LOCK_RETRY_DELAY_MS: u64 = 40;
const STREAM_IDEMPOTENCY_TTL_SECS: u64 = 60 * 60;
const MAX_IDEMPOTENT_CACHE_ENTRIES: usize = 1_024;
const EVENT_TYPE_EXECUTION_STEP: &str = "EXECUTION_STEP";
const MAIN_BRANCH_ID: &str = "br_main";
const DEFAULT_NOTION_API_BASE_URL: &str = "https://api.notion.com/v1";
const DEFAULT_NOTION_API_VERSION: &str = "2022-06-28";
const DEFAULT_NOTION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_NOTION_MAX_ATTEMPTS: u32 = 3;
const MAX_WORKFLOW_JOB_ATTEMPTS: u32 = 3;

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
    #[serde(default)]
    pub callback_url: Option<String>,
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
    pub has_more: bool,
    pub next_cursor: Option<String>,
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
pub struct WorkflowQueueDepthResponse {
    pub depth: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowTerminateResponse {
    pub workflow_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedWorkflowJob {
    pub workflow_id: String,
    pub session_id: String,
    pub step: String,
    pub attempt: u32,
    pub failed_at: u64,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowDlqResponse {
    pub jobs: Vec<FailedWorkflowJob>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct NotionSyncExecutionResult {
    pub session_id: String,
    pub artifacts_count: usize,
    pub summary_preview: String,
    pub status: String,
    pub detail: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    callback_url: Option<String>,
}

#[derive(Debug, Clone)]
struct WorkflowJob {
    workflow_id: String,
    session_id: String,
    step: String,
    payload: String,
    timestamp_ms: u64,
    attempt: u32,
    durable_entry_id: Option<String>,
}

#[derive(Debug, Clone)]
struct NotionSyncJob {
    session_id: String,
    summary: String,
    artifacts: Vec<String>,
    timestamp_ms: u64,
    attempt: u32,
    durable_entry_id: Option<String>,
}

#[derive(Debug, Clone)]
enum NotionSyncOutcome {
    Synced,
    Skipped(String),
}

#[derive(Debug, Clone)]
struct NotionSyncConfig {
    enabled: bool,
    api_base_url: String,
    api_version: String,
    token: Option<String>,
    database_id: Option<String>,
    parent_page_id: Option<String>,
    title_property: String,
    timeout_ms: u64,
    max_attempts: u32,
}

impl NotionSyncConfig {
    fn from_env() -> Self {
        let enabled = read_bool_env("NOTION_SYNC_ENABLED", false);
        let api_base_url = read_string_env("NOTION_API_BASE_URL")
            .unwrap_or_else(|| DEFAULT_NOTION_API_BASE_URL.to_owned());
        let api_version = read_string_env("NOTION_API_VERSION")
            .unwrap_or_else(|| DEFAULT_NOTION_API_VERSION.to_owned());
        let token = read_string_env("NOTION_TOKEN");
        let database_id = read_string_env("NOTION_DATABASE_ID");
        let parent_page_id = read_string_env("NOTION_PARENT_PAGE_ID");
        let title_property =
            read_string_env("NOTION_DATABASE_TITLE_PROPERTY").unwrap_or_else(|| "Name".to_owned());
        let timeout_ms = read_u64_env("NOTION_TIMEOUT_MS", DEFAULT_NOTION_TIMEOUT_MS);
        let max_attempts = read_u32_env("NOTION_SYNC_MAX_ATTEMPTS", DEFAULT_NOTION_MAX_ATTEMPTS);
        Self {
            enabled,
            api_base_url,
            api_version,
            token,
            database_id,
            parent_page_id,
            title_property,
            timeout_ms,
            max_attempts: max_attempts.max(1),
        }
    }

    fn is_active(&self) -> bool {
        if !self.enabled {
            return false;
        }
        if self.token.is_none() {
            return false;
        }
        self.database_id.is_some() || self.parent_page_id.is_some()
    }
}

#[derive(Debug, Clone, Copy)]
enum WorkflowJobSource {
    Durable,
    InMemory,
}

#[derive(Debug, Clone, Copy)]
enum SessionLockBackend {
    Distributed,
    InMemory,
}

#[derive(Debug, Clone)]
struct SessionLockHandle {
    key: String,
    owner: String,
    backend: SessionLockBackend,
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

struct LazyRepo(TokioOnceCell<RedisVddabRepository>);

impl std::fmt::Debug for LazyRepo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyRepo(..)")
    }
}

#[derive(Debug)]
pub struct ExecutionPipeline {
    agent_runtime_config: AgentRuntimeConfig,
    notion_sync_config: NotionSyncConfig,
    notion_http_client: reqwest::Client,
    keyspace: RedisKeyspace,
    persistence: Option<PersistenceConfig>,
    id_counter: AtomicU64,
    workspaces: RwLock<HashMap<String, WorkspaceRecord>>,
    workspace_sessions: RwLock<HashMap<String, Vec<(u64, String)>>>,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    deltashots: RwLock<HashMap<String, DeltashotRecord>>,
    artifacts: RwLock<HashMap<String, ArtifactRecord>>,
    workflows: RwLock<HashMap<String, WorkflowRecord>>,
    workflow_queue: Mutex<VecDeque<WorkflowJob>>,
    workflow_dlq: Mutex<VecDeque<FailedWorkflowJob>>,
    agent_logs: RwLock<HashMap<String, Vec<AgentLogEntry>>>,
    notion_queue: Mutex<VecDeque<NotionSyncJob>>,
    locks: Mutex<HashMap<String, (String, Instant)>>,
    idempotent_streams: Mutex<HashMap<String, Vec<StreamEventEnvelope>>>,
    persistence_hydration: TokioOnceCell<()>,
    persistence_repo: LazyRepo,
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
        let notion_sync_config = NotionSyncConfig::from_env();
        let notion_http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(notion_sync_config.timeout_ms))
            .build()
            .map_err(|err| {
                PipelineError::Internal(format!("notion HTTP client setup failed: {err}"))
            })?;

        Ok(Self {
            agent_runtime_config: AgentRuntimeConfig::from_env(),
            notion_sync_config,
            notion_http_client,
            keyspace,
            persistence: load_persistence_config(),
            id_counter: AtomicU64::new(1),
            workspaces: RwLock::new(HashMap::new()),
            workspace_sessions: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            deltashots: RwLock::new(HashMap::new()),
            artifacts: RwLock::new(HashMap::new()),
            workflows: RwLock::new(HashMap::new()),
            workflow_queue: Mutex::new(VecDeque::new()),
            workflow_dlq: Mutex::new(VecDeque::new()),
            agent_logs: RwLock::new(HashMap::new()),
            notion_queue: Mutex::new(VecDeque::new()),
            locks: Mutex::new(HashMap::new()),
            idempotent_streams: Mutex::new(HashMap::new()),
            persistence_hydration: TokioOnceCell::new(),
            persistence_repo: LazyRepo(TokioOnceCell::new()),
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
        workspaces.insert(workspace_id.clone(), record.clone());
        drop(workspaces);
        self.persist_workspace_record(&record).await;

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
            sessions.insert(session_id.clone(), record.clone());
        }

        {
            let mut links = self.workspace_sessions.write().await;
            links
                .entry(request.workspace_id.clone())
                .or_default()
                .push((created_at, session_id.clone()));
        }

        let workspace_after_session_link = {
            let mut workspaces = self.workspaces.write().await;
            if let Some(ws) = workspaces.get_mut(&request.workspace_id) {
                ws.active_session_id = Some(session_id.clone());
                Some(ws.clone())
            } else {
                None
            }
        };
        if let Some(workspace) = workspace_after_session_link.as_ref() {
            self.persist_workspace_record(workspace).await;
        }
        self.persist_workspace_session_link(&request.workspace_id, &session_id, created_at)
            .await;
        self.persist_session_meta_record(&record).await;
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

    async fn ensure_session_exists(&self, session_id: &str) -> Result<(), PipelineError> {
        let sessions = self.sessions.read().await;
        if sessions.contains_key(session_id) {
            Ok(())
        } else {
            Err(PipelineError::NotFound(format!(
                "session '{}' not found",
                session_id
            )))
        }
    }

    pub async fn handle_message_v1(
        &self,
        session_id: &str,
        request: SendMessageRequestV1,
    ) -> Result<SendMessageResponseV1, PipelineError> {
        self.ensure_persistence_hydrated().await;
        self.ensure_session_exists(session_id).await?;

        if request.content.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "message content cannot be empty".to_owned(),
            ));
        }

        let lock_handle = self.acquire_session_lock(session_id).await?;
        self.record_execution_step(
            session_id,
            "API_ENTRY",
            serde_json::json!({
                "route": "POST /api/v1/sessions/{sessionId}/messages",
            }),
        )
        .await?;
        self.record_execution_step(session_id, "SESSION_HYDRATE_START", Value::Null)
            .await?;
        self.record_execution_step(
            session_id,
            "LOCK_ACQUIRED",
            serde_json::json!({ "lock_key": lock_handle.key.clone() }),
        )
        .await?;

        let result = self.run_locked_message_pipeline(session_id, request).await;
        self.release_lock(&lock_handle).await;
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
        let context = self.hydrate_context(session_id).await?;
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

        let agent_output = self
            .resolve_agent_output(session_id, &request, &context, agent)
            .await?;
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
            let terminal = self.is_workflow_terminal(&workflow_id).await;
            if !terminal {
                self.enqueue_workflow(&workflow_id, session_id, &workflow_step, Value::Null)
                    .await;
                self.bind_session_workflow(session_id, &workflow_id).await;
            }
            self.record_execution_step(
                session_id,
                if terminal { "WORKFLOW_TERMINAL_SKIP" } else { "WORKFLOW_ENQUEUED" },
                serde_json::json!({ "workflow_id": workflow_id, "step": workflow_step.clone() }),
            )
            .await?;
        }

        let notion_enqueued = self
            .enqueue_notion_sync(session_id, &agent_output.text, &artifacts)
            .await;
        self.record_execution_step(
            session_id,
            if notion_enqueued {
                "NOTION_SYNC_ENQUEUED"
            } else {
                "NOTION_SYNC_SKIPPED"
            },
            Value::Null,
        )
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
    ) -> Result<SessionMessagesResponse, PipelineError> {
        let sessions = self.sessions.read().await;
        let Some(session) = sessions.get(session_id) else {
            return Err(PipelineError::NotFound(format!(
                "session '{}' not found",
                session_id
            )));
        };

        let mut messages = session.messages.clone();
        if let Some(cursor_id) = &cursor {
            if let Some(index) = messages.iter().position(|m| &m.id == cursor_id) {
                messages.truncate(index);
            }
        }

        let capped = limit.max(1);
        let has_more = messages.len() > capped;
        let page = messages
            .into_iter()
            .rev()
            .take(capped)
            .map(|m| MessageRecord {
                id: m.id,
                role: m.role,
                content: m.content,
                timestamp: m.timestamp,
            })
            .collect::<Vec<_>>();

        let next_cursor = if has_more {
            page.last().map(|m| m.id.clone())
        } else {
            None
        };

        Ok(SessionMessagesResponse {
            messages: page,
            has_more,
            next_cursor,
        })
    }

    pub async fn list_deltashots(
        &self,
        session_id: &str,
    ) -> Result<Vec<DeltashotView>, PipelineError> {
        let sessions = self.sessions.read().await;
        let Some(session) = sessions.get(session_id) else {
            return Err(PipelineError::NotFound(format!(
                "session '{}' not found",
                session_id
            )));
        };
        let ids = session.deltashot_ids.clone();
        drop(sessions);

        let map = self.deltashots.read().await;
        Ok(ids
            .iter()
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
            .collect::<Vec<_>>())
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
        let lock_handle = self.acquire_session_lock(session_id).await?;

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
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            });
            let session = session?;
            ensure_session_branches(session);
            session.state = rebuilt_state.clone();

            if mode == "hard" {
                session.deltashot_ids.truncate(target_index + 1);
                session.hashchain.truncate(target_index + 1);
            }
            sync_active_branch_from_session(session);
            let active_branch_id = session.active_branch_id.clone();
            let truncated_chain = session.deltashot_ids.clone();

            drop(sessions);

            if mode == "hard" {
                self.persist_branch_chain_truncation(
                    session_id,
                    &active_branch_id,
                    &truncated_chain,
                )
                .await;
            }

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

        self.release_lock(&lock_handle).await;
        result
    }

    pub async fn create_or_update_artifact_api(
        &self,
        request: ArtifactWriteRequest,
    ) -> Result<ArtifactEnvelope, PipelineError> {
        self.ensure_persistence_hydrated().await;
        self.ensure_session_exists(&request.session_id).await?;
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
        self.ensure_session_exists(&request.session_id).await?;
        let now = now_ms();
        let record = WorkflowRecord {
            workflow_id: request.workflow_id.clone(),
            session_id: request.session_id.clone(),
            status: "active".to_owned(),
            current_step: "start".to_owned(),
            updated_at: now,
            callback_url: request.callback_url.filter(|u| !u.is_empty()),
        };

        {
            let mut workflows = self.workflows.write().await;
            workflows.insert(request.workflow_id.clone(), record.clone());
        }
        self.persist_workflow_record(&record).await;

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

    pub async fn terminate_workflow(
        &self,
        workflow_id: &str,
        terminal_status: &str,
    ) -> Result<WorkflowTerminateResponse, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let snapshot = {
            let mut workflows = self.workflows.write().await;
            let record = workflows.get_mut(workflow_id).ok_or_else(|| {
                PipelineError::NotFound(format!("workflow '{}' not found", workflow_id))
            })?;
            record.status = terminal_status.to_owned();
            record.updated_at = now_ms();
            record.clone()
        };
        self.persist_workflow_record(&snapshot).await;
        if let Some(url) = snapshot.callback_url {
            self.fire_workflow_callback(workflow_id, terminal_status, &url)
                .await;
        }
        Ok(WorkflowTerminateResponse {
            workflow_id: workflow_id.to_owned(),
            status: terminal_status.to_owned(),
        })
    }

    pub async fn workflow_queue_depth(&self) -> WorkflowQueueDepthResponse {
        let queue = self.workflow_queue.lock().await;
        WorkflowQueueDepthResponse { depth: queue.len() }
    }

    pub async fn get_workflow_dlq(&self) -> WorkflowDlqResponse {
        let dlq = self.workflow_dlq.lock().await;
        let jobs: Vec<FailedWorkflowJob> = dlq.iter().cloned().collect();
        let total = jobs.len();
        WorkflowDlqResponse { jobs, total }
    }

    async fn fire_workflow_callback(&self, workflow_id: &str, status: &str, url: &str) {
        let client = self.notion_http_client.clone();
        let url = url.to_owned();
        let wid = workflow_id.to_owned();
        let status = status.to_owned();
        tokio::spawn(async move {
            let payload = serde_json::json!({
                "workflow_id": wid,
                "status": status,
                "fired_at": now_ms(),
            });
            match client.post(&url).json(&payload).send().await {
                Ok(resp) => {
                    info!(workflow_id = %wid, http_status = %resp.status(), "workflow callback delivered");
                }
                Err(err) => {
                    warn!(workflow_id = %wid, error = %err, "workflow callback delivery failed");
                }
            }
        });
    }

    async fn is_workflow_terminal(&self, workflow_id: &str) -> bool {
        let workflows = self.workflows.read().await;
        workflows
            .get(workflow_id)
            .map(|r| matches!(r.status.as_str(), "completed" | "cancelled"))
            .unwrap_or(false)
    }

    pub async fn advance_workflow_step(
        &self,
        workflow_id: &str,
        request: WorkflowStepRequest,
    ) -> Result<WorkflowStateView, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let (session_id, workflow_snapshot) = {
            let mut workflows = self.workflows.write().await;
            let record = workflows.get_mut(workflow_id).ok_or_else(|| {
                PipelineError::NotFound(format!("workflow '{}' not found", workflow_id))
            })?;
            record.current_step = request.step.clone();
            record.updated_at = now_ms();
            (record.session_id.clone(), record.clone())
        };
        self.persist_workflow_record(&workflow_snapshot).await;

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
        let (next_job, source) = self.next_workflow_job().await;

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
            Ok(response) => {
                self.ack_workflow_job_if_needed(&job, source).await;
                Ok(Some(WorkflowExecutionResult {
                    workflow_id: job.workflow_id,
                    session_id: job.session_id,
                    step: job.step,
                    deltashot_id: response.deltashot.id,
                    state_version: response.state.version,
                }))
            }
            Err(error) => {
                let next_attempt = job.attempt.saturating_add(1);
                if matches!(error, PipelineError::LockUnavailable)
                    || next_attempt < MAX_WORKFLOW_JOB_ATTEMPTS
                {
                    self.requeue_workflow_job(job, source).await;
                } else {
                    self.ack_workflow_job_if_needed(&job, source).await;
                    let failed = FailedWorkflowJob {
                        workflow_id: job.workflow_id.clone(),
                        session_id: job.session_id.clone(),
                        step: job.step.clone(),
                        attempt: job.attempt,
                        failed_at: now_ms(),
                        error: error.to_string(),
                    };
                    warn!(
                        workflow_id = %failed.workflow_id,
                        attempt = failed.attempt,
                        error = %failed.error,
                        "workflow job exhausted retries, moving to dead-letter queue"
                    );
                    self.workflow_dlq.lock().await.push_back(failed);
                }
                Err(error)
            }
        }
    }

    pub async fn execute_next_notion_sync_job(
        &self,
    ) -> Result<Option<NotionSyncExecutionResult>, PipelineError> {
        self.ensure_persistence_hydrated().await;
        let (next_job, source) = self.next_notion_sync_job().await;
        let Some(job) = next_job else {
            return Ok(None);
        };

        let (status, detail) = match self.perform_notion_sync(&job).await {
            Ok(NotionSyncOutcome::Synced) => {
                ("synced".to_owned(), "notion page updated".to_owned())
            }
            Ok(NotionSyncOutcome::Skipped(reason)) => ("skipped".to_owned(), reason),
            Err(error) => {
                warn!(
                    session_id = %job.session_id,
                    attempt = job.attempt,
                    error = %error,
                    "notion sync execution failed"
                );
                self.requeue_notion_sync_job(job.clone(), source).await;
                return Err(error);
            }
        };
        self.ack_notion_job_if_needed(&job, source).await;

        let preview = job.summary.chars().take(140).collect::<String>();
        info!(
            session_id = %job.session_id,
            artifacts_count = job.artifacts.len(),
            attempt = job.attempt,
            "executed notion sync background job"
        );

        Ok(Some(NotionSyncExecutionResult {
            session_id: job.session_id,
            artifacts_count: job.artifacts.len(),
            summary_preview: preview,
            status,
            detail,
        }))
    }

    async fn perform_notion_sync(
        &self,
        job: &NotionSyncJob,
    ) -> Result<NotionSyncOutcome, PipelineError> {
        if !self.notion_sync_config.is_active() {
            info!(
                session_id = %job.session_id,
                "notion sync skipped because integration is not configured"
            );
            return Ok(NotionSyncOutcome::Skipped(
                "integration disabled or incomplete".to_owned(),
            ));
        }

        let Some(token) = self.notion_sync_config.token.as_ref() else {
            return Ok(NotionSyncOutcome::Skipped(
                "missing NOTION_TOKEN".to_owned(),
            ));
        };

        let mut properties = serde_json::Map::new();
        let title = format!("Delta Agent Sync {}", job.session_id);
        let title_property = if self.notion_sync_config.parent_page_id.is_some() {
            "title".to_owned()
        } else {
            self.notion_sync_config.title_property.clone()
        };
        properties.insert(title_property, notion_title_property(&title));

        let parent = if let Some(parent_page_id) = self.notion_sync_config.parent_page_id.as_ref() {
            serde_json::json!({ "page_id": parent_page_id })
        } else if let Some(database_id) = self.notion_sync_config.database_id.as_ref() {
            serde_json::json!({ "database_id": database_id })
        } else {
            return Ok(NotionSyncOutcome::Skipped(
                "missing NOTION_DATABASE_ID or NOTION_PARENT_PAGE_ID".to_owned(),
            ));
        };

        let summary_preview = truncate_for_notion(&job.summary, 1800);
        let artifacts_line = if job.artifacts.is_empty() {
            "Artifacts: none".to_owned()
        } else {
            format!(
                "Artifacts: {}",
                truncate_for_notion(&job.artifacts.join(", "), 1800)
            )
        };
        let timestamp_line = format!("Queued at: {}", job.timestamp_ms);
        let body = serde_json::json!({
            "parent": parent,
            "properties": properties,
            "children": [
                notion_paragraph_block(&format!("Session: {}", job.session_id)),
                notion_paragraph_block(&timestamp_line),
                notion_paragraph_block(&artifacts_line),
                notion_paragraph_block(&format!("Summary: {}", summary_preview)),
            ],
        });

        let endpoint = format!(
            "{}/pages",
            self.notion_sync_config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .notion_http_client
            .post(&endpoint)
            .bearer_auth(token)
            .header(
                "Notion-Version",
                self.notion_sync_config.api_version.as_str(),
            )
            .json(&body)
            .send()
            .await
            .map_err(|err| PipelineError::Internal(format!("notion request failed: {err}")))?;

        if response.status().is_success() {
            return Ok(NotionSyncOutcome::Synced);
        }

        let status = response.status();
        let raw = response.text().await.unwrap_or_default();
        Err(PipelineError::Internal(format!(
            "notion returned status {}: {}",
            status,
            truncate_for_notion(&raw, 400)
        )))
    }

    async fn requeue_notion_sync_job(&self, mut job: NotionSyncJob, source: WorkflowJobSource) {
        self.ack_notion_job_if_needed(&job, source).await;
        job.attempt += 1;
        job.durable_entry_id = None;
        if job.attempt >= self.notion_sync_config.max_attempts {
            warn!(
                session_id = %job.session_id,
                attempt = job.attempt,
                max_attempts = self.notion_sync_config.max_attempts,
                "dropping notion sync job after reaching max retry attempts"
            );
            return;
        }

        if self.enqueue_notion_sync_durable(&job).await {
            info!(
                session_id = %job.session_id,
                attempt = job.attempt,
                "requeued notion sync job in durable queue"
            );
            return;
        }

        let mut queue = self.notion_queue.lock().await;
        queue.push_back(job.clone());
        info!(
            session_id = %job.session_id,
            attempt = job.attempt,
            source = ?source,
            "requeued notion sync job in memory queue"
        );
    }

    pub async fn force_agent_selection(
        &self,
        session_id: &str,
        request: ForceAgentRequest,
    ) -> Result<AgentSelectionView, PipelineError> {
        self.ensure_persistence_hydrated().await;
        self.ensure_session_exists(session_id).await?;
        let canonical = normalize_agent_name(&request.agent).ok_or_else(|| {
            PipelineError::InvalidInput("agent must be one of: claude | deepseek".to_owned())
        })?;

        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        session.forced_agent = Some(ForcedAgent {
            agent: canonical.to_owned(),
            reason: request.reason.clone(),
        });
        let snapshot = session.clone();
        drop(sessions);
        self.persist_session_meta_record(&snapshot).await;

        Ok(AgentSelectionView {
            session_id: session_id.to_owned(),
            agent: canonical.to_owned(),
            reason: request.reason,
        })
    }

    pub async fn get_agent_logs(
        &self,
        session_id: &str,
    ) -> Result<Vec<AgentLogEntry>, PipelineError> {
        self.ensure_session_exists(session_id).await?;
        let mut merged = {
            let logs = self.agent_logs.read().await;
            logs.get(session_id).cloned().unwrap_or_default()
        };
        if let Some(repo) = self.persistence_backend().await {
            match repo.load_agent_logs(session_id).await {
                Ok(persisted) => {
                    merged.extend(persisted.into_iter().map(|entry| AgentLogEntry {
                        ts: entry.ts,
                        selected_agent: entry.selected_agent,
                        reason: entry.reason,
                    }));
                }
                Err(err) => {
                    warn!(
                        session_id = %session_id,
                        error = %err,
                        "failed to load persisted agent logs"
                    );
                }
            }
        }
        merged.sort_by(|a, b| {
            a.ts.cmp(&b.ts)
                .then_with(|| a.selected_agent.cmp(&b.selected_agent))
                .then_with(|| a.reason.cmp(&b.reason))
        });
        merged.dedup_by(|a, b| {
            a.ts == b.ts && a.selected_agent == b.selected_agent && a.reason == b.reason
        });
        Ok(merged)
    }

    pub async fn get_trace(&self, session_id: &str) -> Result<ExecutionTrace, PipelineError> {
        let session_opt = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        let Some(session) = session_opt else {
            return Err(PipelineError::NotFound(format!(
                "session '{}' not found",
                session_id
            )));
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

        Ok(ExecutionTrace {
            messages,
            deltashots: self.list_deltashots(session_id).await?,
            state_transitions,
            artifacts,
        })
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
        let Some(config) = self.persistence.as_ref() else {
            return Err(PipelineError::InvalidInput(
                "persistence backend is not configured".to_owned(),
            ));
        };

        let repo = RedisVddabRepository::connect(
            &config.redis_url,
            self.keyspace.clone(),
            config.vddab_root.clone(),
        )
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
        let repo = self
            .persistence_repo
            .0
            .get_or_try_init(|| async {
                RedisVddabRepository::connect(
                    &config.redis_url,
                    self.keyspace.clone(),
                    &config.vddab_root,
                )
                .await
            })
            .await
            .map_err(|err| {
                warn!(error = %err, "failed to initialize persistence backend");
                err
            })
            .ok()?;
        Some(repo.clone())
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
        let mut recovered_artifacts = HashMap::<String, ArtifactRecord>::new();
        let mut recovered_agent_logs = HashMap::<String, Vec<AgentLogEntry>>::new();

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
            let branch_id_set = branch_ids.iter().cloned().collect::<HashSet<_>>();
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
            if let Ok(Some(meta)) = repo.get_session_meta(&session_id).await {
                session._workspace_id = meta.workspace_id;
                session._name = meta.name;
                session.created_at = meta.created_at;
                session.status = meta.status;
                session.workflow_id = meta.workflow_id;
                session.forced_agent = meta.forced_agent.map(|agent| ForcedAgent {
                    agent,
                    reason: meta
                        .forced_agent_reason
                        .unwrap_or_else(|| "persisted".to_owned()),
                });
            }
            match repo.load_agent_logs(&session_id).await {
                Ok(mut entries) => {
                    entries.sort_by(|a, b| {
                        a.ts.cmp(&b.ts)
                            .then_with(|| a.selected_agent.cmp(&b.selected_agent))
                            .then_with(|| a.reason.cmp(&b.reason))
                    });
                    recovered_agent_logs.insert(
                        session_id.clone(),
                        entries
                            .into_iter()
                            .map(|entry| AgentLogEntry {
                                ts: entry.ts,
                                selected_agent: entry.selected_agent,
                                reason: entry.reason,
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                Err(err) => {
                    warn!(
                        session_id = %session_id,
                        error = %err,
                        "failed to hydrate session agent logs"
                    );
                }
            }
            let mut branch_messages = HashMap::<String, Vec<ChatMessage>>::new();
            match repo.load_session_messages(&session_id).await {
                Ok(messages) => {
                    for message in messages {
                        update_max_id_seed(&mut max_id, &message.id);
                        let branch_id = if message.branch_id.is_empty()
                            || !branch_id_set.contains(&message.branch_id)
                        {
                            MAIN_BRANCH_ID.to_owned()
                        } else {
                            message.branch_id
                        };
                        branch_messages
                            .entry(branch_id)
                            .or_default()
                            .push(ChatMessage {
                                id: message.id,
                                role: message.role,
                                content: message.content,
                                timestamp: message.timestamp,
                            });
                    }
                }
                Err(err) => {
                    warn!(
                        session_id = %session_id,
                        error = %err,
                        "failed to hydrate session messages"
                    );
                }
            }
            for branch in branch_messages.values_mut() {
                branch.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
            }

            let mut branch_events = HashMap::<String, Vec<SessionEvent>>::new();
            match repo.load_session_events(&session_id).await {
                Ok(events) => {
                    for event in events {
                        update_max_id_seed(&mut max_id, &event.id);
                        let branch_id = if event.branch_id.is_empty()
                            || !branch_id_set.contains(&event.branch_id)
                        {
                            MAIN_BRANCH_ID.to_owned()
                        } else {
                            event.branch_id
                        };
                        branch_events
                            .entry(branch_id)
                            .or_default()
                            .push(SessionEvent {
                                id: event.id,
                                event_type: event.event_type,
                                payload: event.payload,
                                timestamp: event.timestamp,
                            });
                    }
                }
                Err(err) => {
                    warn!(
                        session_id = %session_id,
                        error = %err,
                        "failed to hydrate session events"
                    );
                }
            }
            for branch in branch_events.values_mut() {
                branch.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
            }

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
                    messages: branch_messages.remove(&branch_id).unwrap_or_default(),
                    events: branch_events.remove(&branch_id).unwrap_or_default(),
                    hashchain,
                    deltashot_ids,
                    artifacts: HashSet::new(),
                };
                session.branches.insert(branch_id, branch);
            }

            if !session.branches.contains_key(&session.active_branch_id) {
                session.active_branch_id = MAIN_BRANCH_ID.to_owned();
            }
            let mut branch_artifacts = HashMap::<String, HashSet<String>>::new();
            if let Ok(artifact_ids) = repo.list_session_artifacts(&session_id).await {
                for artifact_id in artifact_ids {
                    update_max_id_seed(&mut max_id, &artifact_id);
                    if let Ok(Some(record)) = repo.get_artifact(&artifact_id).await {
                        let artifact_branch_id = if record.branch_id.is_empty()
                            || !branch_id_set.contains(&record.branch_id)
                        {
                            MAIN_BRANCH_ID.to_owned()
                        } else {
                            record.branch_id.clone()
                        };
                        let versions = record
                            .versions
                            .into_iter()
                            .map(|version| (version.version, version.content))
                            .collect::<BTreeMap<_, _>>();
                        recovered_artifacts.insert(
                            artifact_id.clone(),
                            ArtifactRecord {
                                artifact_id: record.artifact_id.clone(),
                                session_id: record.session_id.clone(),
                                branch_id: artifact_branch_id.clone(),
                                artifact_type: record.artifact_type,
                                created_at: record.created_at,
                                current_version: record.current_version,
                                versions,
                            },
                        );
                        branch_artifacts
                            .entry(artifact_branch_id)
                            .or_default()
                            .insert(artifact_id.clone());
                    }
                }
            }
            for branch in session.branches.values_mut() {
                branch.artifacts = branch_artifacts
                    .remove(&branch.branch_id)
                    .unwrap_or_default();
            }
            let active_branch_id = session.active_branch_id.clone();
            if load_branch_into_session(&mut session, &active_branch_id).is_err() {
                session.active_branch_id = MAIN_BRANCH_ID.to_owned();
                let _ = load_branch_into_session(&mut session, MAIN_BRANCH_ID);
            }
            recovered_sessions.insert(session_id, session);
        }

        let mut recovered_workspaces = HashMap::<String, WorkspaceRecord>::new();
        let mut recovered_workspace_sessions = HashMap::<String, Vec<(u64, String)>>::new();
        if let Ok(workspaces) = repo.list_workspaces().await {
            for workspace in workspaces {
                update_max_id_seed(&mut max_id, &workspace.workspace_id);
                let workspace_id = workspace.workspace_id.clone();
                recovered_workspaces.insert(
                    workspace_id.clone(),
                    WorkspaceRecord {
                        workspace_id: workspace.workspace_id,
                        name: workspace.name,
                        created_at: workspace.created_at,
                        owner_id: workspace.owner_id,
                        active_session_id: workspace.active_session_id,
                    },
                );
                if let Ok(links) = repo.get_workspace_sessions(&workspace_id).await {
                    recovered_workspace_sessions.insert(workspace_id, links);
                }
            }
        }
        for session in recovered_sessions.values() {
            let workspace_id = session._workspace_id.clone();
            if workspace_id.is_empty() {
                continue;
            }
            recovered_workspaces
                .entry(workspace_id.clone())
                .or_insert_with(|| WorkspaceRecord {
                    workspace_id: workspace_id.clone(),
                    name: "Rehydrated Workspace".to_owned(),
                    created_at: session.created_at,
                    owner_id: "system".to_owned(),
                    active_session_id: Some(session.session_id.clone()),
                });
            recovered_workspace_sessions
                .entry(workspace_id)
                .or_default()
                .push((session.created_at, session.session_id.clone()));
        }

        let mut recovered_workflows = HashMap::<String, WorkflowRecord>::new();
        if let Ok(workflow_ids) = repo.list_workflow_ids().await {
            for workflow_id in workflow_ids {
                if let Ok(Some(workflow)) = repo.get_workflow_state(&workflow_id).await {
                    recovered_workflows.insert(
                        workflow_id.clone(),
                        WorkflowRecord {
                            workflow_id,
                            session_id: workflow.session_id,
                            status: workflow.status,
                            current_step: workflow.current_step,
                            updated_at: workflow.updated_at,
                            callback_url: None,
                        },
                    );
                }
            }
        }
        for entries in recovered_workspace_sessions.values_mut() {
            entries.sort_by_key(|(ts, session_id)| (*ts, session_id.clone()));
            entries.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
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
        {
            let mut artifacts = self.artifacts.write().await;
            for (artifact_id, record) in recovered_artifacts {
                artifacts.entry(artifact_id).or_insert(record);
            }
        }
        {
            let mut workflows = self.workflows.write().await;
            for (workflow_id, record) in recovered_workflows {
                workflows.entry(workflow_id).or_insert(record);
            }
        }
        {
            let mut workspaces = self.workspaces.write().await;
            for (workspace_id, record) in recovered_workspaces {
                workspaces.entry(workspace_id).or_insert(record);
            }
        }
        {
            let mut links = self.workspace_sessions.write().await;
            for (workspace_id, entries) in recovered_workspace_sessions {
                links.entry(workspace_id).or_insert(entries);
            }
        }
        {
            let mut logs = self.agent_logs.write().await;
            for (session_id, entries) in recovered_agent_logs {
                logs.entry(session_id).or_insert(entries);
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

    async fn persist_branch_chain_truncation(
        &self,
        session_id: &str,
        branch_id: &str,
        remaining_ids: &[String],
    ) {
        if let Some(repo) = self.persistence_backend().await {
            let storage_branch_id = persistence_branch_key(session_id, branch_id);
            if let Err(err) = repo
                .replace_branch_chain(&storage_branch_id, remaining_ids)
                .await
            {
                warn!(
                    session_id = %session_id,
                    branch_id = %branch_id,
                    error = %err,
                    "failed to persist branch chain truncation"
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

    async fn persist_workspace_record(&self, workspace: &WorkspaceRecord) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedWorkspaceRecord {
                workspace_id: workspace.workspace_id.clone(),
                name: workspace.name.clone(),
                created_at: workspace.created_at,
                owner_id: workspace.owner_id.clone(),
                active_session_id: workspace.active_session_id.clone(),
            };
            if let Err(err) = repo.store_workspace(&persisted).await {
                warn!(
                    workspace_id = %workspace.workspace_id,
                    error = %err,
                    "failed to persist workspace record"
                );
            }
        }
    }

    async fn persist_workspace_session_link(
        &self,
        workspace_id: &str,
        session_id: &str,
        created_at: u64,
    ) {
        if let Some(repo) = self.persistence_backend().await {
            if let Err(err) = repo
                .link_workspace_session(workspace_id, session_id, created_at)
                .await
            {
                warn!(
                    workspace_id = %workspace_id,
                    session_id = %session_id,
                    error = %err,
                    "failed to persist workspace/session link"
                );
            }
        }
    }

    async fn persist_session_meta_record(&self, session: &SessionRecord) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedSessionMeta {
                session_id: session.session_id.clone(),
                workspace_id: session._workspace_id.clone(),
                name: session._name.clone(),
                created_at: session.created_at,
                status: session.status.clone(),
                workflow_id: session.workflow_id.clone(),
                forced_agent: session
                    .forced_agent
                    .as_ref()
                    .map(|forced| forced.agent.clone()),
                forced_agent_reason: session
                    .forced_agent
                    .as_ref()
                    .map(|forced| forced.reason.clone()),
            };
            if let Err(err) = repo.store_session_meta(&persisted).await {
                warn!(
                    session_id = %session.session_id,
                    error = %err,
                    "failed to persist session metadata"
                );
            }
        }
    }

    async fn persist_session_message(
        &self,
        session_id: &str,
        branch_id: &str,
        message: &ChatMessage,
    ) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedSessionMessage {
                id: message.id.clone(),
                branch_id: branch_id.to_owned(),
                role: message.role.clone(),
                content: message.content.clone(),
                timestamp: message.timestamp,
            };
            if let Err(err) = repo.append_session_message(session_id, &persisted).await {
                warn!(
                    session_id = %session_id,
                    message_id = %message.id,
                    error = %err,
                    "failed to persist session message"
                );
            }
        }
    }

    async fn persist_session_event(&self, session_id: &str, branch_id: &str, event: &SessionEvent) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedSessionEvent {
                id: event.id.clone(),
                branch_id: branch_id.to_owned(),
                event_type: event.event_type.clone(),
                payload: event.payload.clone(),
                timestamp: event.timestamp,
            };
            if let Err(err) = repo.append_session_event(session_id, &persisted).await {
                warn!(
                    session_id = %session_id,
                    event_id = %event.id,
                    error = %err,
                    "failed to persist session event"
                );
            }
        }
    }

    async fn persist_workflow_record(&self, workflow: &WorkflowRecord) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedWorkflowRecord {
                workflow_id: workflow.workflow_id.clone(),
                session_id: workflow.session_id.clone(),
                status: workflow.status.clone(),
                current_step: workflow.current_step.clone(),
                updated_at: workflow.updated_at,
            };
            if let Err(err) = repo.store_workflow_state(&persisted).await {
                warn!(
                    workflow_id = %workflow.workflow_id,
                    error = %err,
                    "failed to persist workflow state"
                );
            }
        }
    }

    async fn persist_artifact_record(&self, artifact: &ArtifactRecord) {
        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedArtifactRecord {
                artifact_id: artifact.artifact_id.clone(),
                session_id: artifact.session_id.clone(),
                branch_id: artifact.branch_id.clone(),
                artifact_type: artifact.artifact_type.clone(),
                created_at: artifact.created_at,
                current_version: artifact.current_version,
                versions: artifact
                    .versions
                    .iter()
                    .map(|(version, content)| PersistedArtifactVersion {
                        version: *version,
                        content: content.clone(),
                    })
                    .collect::<Vec<_>>(),
            };
            if let Err(err) = repo.store_artifact(&persisted).await {
                warn!(
                    artifact_id = %artifact.artifact_id,
                    error = %err,
                    "failed to persist artifact record"
                );
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
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;

        let mut branches = session
            .branches
            .values()
            .map(|branch| BranchSummary {
                branch_id: branch.branch_id.clone(),
                is_main: branch.branch_id == MAIN_BRANCH_ID,
                label: branch.label.clone(),
            })
            .collect::<Vec<_>>();

        branches.sort_by(|a, b| {
            b.is_main
                .cmp(&a.is_main)
                .then_with(|| a.branch_id.cmp(&b.branch_id))
        });

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
            .and_then(|key| self.stream_idempotency_storage_key(session_id, key));
        if let Some(key) = cache_key.as_ref() {
            if let Some(cached) = self.load_cached_stream_events(key).await {
                return cached.clone();
            }
        }

        let request_id = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("req_{}", uuid::Uuid::new_v4().simple()));
        let mut events = vec![stream_event("ack", StreamAckEvent { request_id })];

        if let Err(error) = self.ensure_session_exists(session_id).await {
            events.push(stream_event(
                "error",
                StreamErrorEvent {
                    code: error.code().to_owned(),
                    retryable: error.retryable(),
                },
            ));
            if let Some(key) = cache_key.as_ref() {
                self.store_cached_stream_events(key, &events).await;
            }
            return events;
        }

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
            if let Some(key) = cache_key.as_ref() {
                self.store_cached_stream_events(key, &events).await;
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
                if let Some(key) = cache_key.as_ref() {
                    self.store_cached_stream_events(key, &events).await;
                }
                return events;
            }
        }

        let lock_handle = match self.acquire_session_lock(session_id).await {
            Ok(key) => key,
            Err(error) => {
                events.push(stream_event(
                    "error",
                    StreamErrorEvent {
                        code: error.code().to_owned(),
                        retryable: error.retryable(),
                    },
                ));
                if let Some(key) = cache_key.as_ref() {
                    self.store_cached_stream_events(key, &events).await;
                }
                return events;
            }
        };

        let result = self
            .run_locked_stream_pipeline(session_id, request, &mut events)
            .await;
        self.release_lock(&lock_handle).await;

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

        if let Some(key) = cache_key.as_ref() {
            self.store_cached_stream_events(key, &events).await;
        }
        events
    }

    async fn run_locked_stream_pipeline(
        &self,
        session_id: &str,
        request: StreamMessageRequestV1,
        events: &mut Vec<StreamEventEnvelope>,
    ) -> Result<(), PipelineError> {
        let context = self.hydrate_context(session_id).await?;
        let _ = self
            .append_message(session_id, "user", &request.content)
            .await?;

        let (agent, reason) = select_agent(
            &request.content,
            &request.mode,
            context.forced_agent.as_ref(),
        );
        self.append_agent_log(session_id, &agent, &reason).await;
        let mut accumulated = if self.agent_runtime_config.use_real_agents {
            let provider = map_agent_kind(agent);
            let router =
                RealAgentRouter::new(self.agent_runtime_config.clone()).map_err(map_agent_error)?;
            let mut stream = router
                .stream(
                    provider,
                    AgentRequest {
                        session_id: session_id.to_owned(),
                        request_id: format!("req_{}", uuid::Uuid::new_v4().simple()),
                        prompt: request.content.clone(),
                        context: context
                            .memory
                            .iter()
                            .rev()
                            .map(|message| ClientAgentMessage {
                                role: message.role.clone(),
                                content: message.content.clone(),
                            })
                            .collect::<Vec<_>>(),
                        metadata: request.metadata.clone(),
                        mode: map_message_mode(&request.mode),
                        model_override: None,
                        max_tokens: None,
                        temperature: None,
                    },
                )
                .await
                .map_err(map_agent_error)?;

            let mut accumulated = String::new();
            let mut saw_done = false;
            while let Some(event) = stream.next().await {
                match event.map_err(map_agent_error)? {
                    crate::agents::types::AgentStreamEvent::Token { delta } => {
                        accumulated.push_str(&delta);
                        events.push(stream_event(
                            "token",
                            StreamTokenEvent {
                                delta,
                                accumulated: accumulated.clone(),
                            },
                        ));
                    }
                    crate::agents::types::AgentStreamEvent::Usage { .. } => {}
                    crate::agents::types::AgentStreamEvent::Done { .. } => {
                        saw_done = true;
                        break;
                    }
                }
            }
            if !saw_done {
                return Err(PipelineError::Internal(
                    "agent stream completed without done event".to_owned(),
                ));
            }
            accumulated
        } else {
            let generated =
                build_local_agent_response(&request.content, &context, &request.mode, agent);
            let mut accumulated = String::new();
            for delta in local_stream_chunks(&generated) {
                accumulated.push_str(&delta);
                events.push(stream_event(
                    "token",
                    StreamTokenEvent {
                        delta,
                        accumulated: accumulated.clone(),
                    },
                ));
            }
            if accumulated.is_empty() {
                generated
            } else {
                accumulated
            }
        };
        if accumulated.trim().is_empty() {
            accumulated =
                build_local_agent_response(&request.content, &context, &request.mode, agent);
        }
        let agent_output =
            build_agent_output_from_text(&request.content, &context, &request.mode, accumulated);

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

    async fn resolve_agent_output(
        &self,
        session_id: &str,
        request: &SendMessageRequestV1,
        context: &SessionContext,
        agent: AgentKind,
    ) -> Result<AgentOutput, PipelineError> {
        if !self.agent_runtime_config.use_real_agents {
            let generated =
                build_local_agent_response(&request.content, context, &request.mode, agent);
            return Ok(build_agent_output_from_text(
                &request.content,
                context,
                &request.mode,
                generated,
            ));
        }

        let agent_request = AgentRequest {
            session_id: session_id.to_owned(),
            request_id: format!("req_{}", uuid::Uuid::new_v4().simple()),
            prompt: request.content.clone(),
            context: context
                .memory
                .iter()
                .rev()
                .map(|message| ClientAgentMessage {
                    role: message.role.clone(),
                    content: message.content.clone(),
                })
                .collect::<Vec<_>>(),
            metadata: request.metadata.clone(),
            mode: map_message_mode(&request.mode),
            model_override: None,
            max_tokens: None,
            temperature: None,
        };
        let provider = map_agent_kind(agent);
        let router =
            RealAgentRouter::new(self.agent_runtime_config.clone()).map_err(map_agent_error)?;
        let result = router
            .complete(provider, agent_request)
            .await
            .map_err(map_agent_error)?;
        info!(
            session_id = %session_id,
            provider = ?result.provider,
            model = %result.model,
            latency_ms = result.latency_ms,
            "real agent completion executed"
        );

        Ok(build_agent_output_from_text(
            &request.content,
            context,
            &request.mode,
            result.text,
        ))
    }

    async fn hydrate_context(&self, session_id: &str) -> Result<SessionContext, PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        ensure_session_branches(session);

        let mut memory = session.messages.clone();
        memory.reverse();
        memory.truncate(RECENT_MESSAGE_WINDOW);

        Ok(SessionContext {
            state: session.state.clone(),
            memory,
            workflow_id: session.workflow_id.clone(),
            forced_agent: session.forced_agent.clone(),
        })
    }

    async fn acquire_session_lock(
        &self,
        session_id: &str,
    ) -> Result<SessionLockHandle, PipelineError> {
        let lock_key = self
            .keyspace
            .lock_session(session_id)
            .map_err(|err| PipelineError::InvalidInput(err.to_string()))?;
        let owner = format!("lock_{}", uuid::Uuid::new_v4().simple());

        for _ in 0..SESSION_LOCK_RETRIES {
            if let Some(backend) = self.try_acquire_lock(&lock_key, &owner).await? {
                return Ok(SessionLockHandle {
                    key: lock_key.clone(),
                    owner: owner.clone(),
                    backend,
                });
            }
            tokio::time::sleep(Duration::from_millis(LOCK_RETRY_DELAY_MS)).await;
        }
        Err(PipelineError::LockUnavailable)
    }

    async fn try_acquire_lock(
        &self,
        lock_key: &str,
        owner: &str,
    ) -> Result<Option<SessionLockBackend>, PipelineError> {
        if self.persistence.is_some() {
            let repo = self.persistence_backend().await.ok_or_else(|| {
                PipelineError::Internal(
                    "distributed lock backend unavailable while persistence is enabled".to_owned(),
                )
            })?;
            let acquired = repo
                .try_acquire_distributed_lock(lock_key, owner, SESSION_LOCK_TTL_SECS)
                .await
                .map_err(|err| {
                    PipelineError::Internal(format!(
                        "distributed lock acquisition failed for '{}': {err}",
                        lock_key
                    ))
                })?;
            return Ok(if acquired {
                Some(SessionLockBackend::Distributed)
            } else {
                None
            });
        }

        let mut locks = self.locks.lock().await;
        let now = Instant::now();
        locks.retain(|_, (_, expires_at)| *expires_at > now);

        if locks.contains_key(lock_key) {
            return Ok(None);
        }

        locks.insert(
            lock_key.to_owned(),
            (
                owner.to_owned(),
                now + Duration::from_secs(SESSION_LOCK_TTL_SECS),
            ),
        );
        Ok(Some(SessionLockBackend::InMemory))
    }

    async fn release_lock(&self, handle: &SessionLockHandle) {
        match handle.backend {
            SessionLockBackend::Distributed => {
                let Some(repo) = self.persistence_backend().await else {
                    warn!(
                        lock_key = %handle.key,
                        "distributed lock backend unavailable during release"
                    );
                    return;
                };
                if let Err(err) = repo
                    .release_distributed_lock(&handle.key, &handle.owner)
                    .await
                {
                    warn!(
                        lock_key = %handle.key,
                        error = %err,
                        "failed to release distributed lock"
                    );
                }
            }
            SessionLockBackend::InMemory => {
                let mut locks = self.locks.lock().await;
                if let Some((current_owner, _)) = locks.get(&handle.key) {
                    if current_owner == &handle.owner {
                        locks.remove(&handle.key);
                    }
                }
            }
        }
    }

    fn stream_idempotency_storage_key(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Option<String> {
        let digest = blake3::hash(idempotency_key.as_bytes())
            .to_hex()
            .chars()
            .take(32)
            .collect::<String>();
        self.keyspace.stream_idempotency(session_id, &digest).ok()
    }

    async fn load_cached_stream_events(&self, cache_key: &str) -> Option<Vec<StreamEventEnvelope>> {
        if self.persistence.is_some() {
            let repo = match self.persistence_backend().await {
                Some(repo) => repo,
                None => {
                    warn!(
                        cache_key = %cache_key,
                        "idempotency backend unavailable while persistence is enabled"
                    );
                    return None;
                }
            };
            let payload = match repo.load_idempotency_payload(cache_key).await {
                Ok(value) => value?,
                Err(err) => {
                    warn!(cache_key = %cache_key, error = %err, "failed to load idempotent stream cache");
                    return None;
                }
            };
            let cached = match serde_json::from_str::<Vec<StreamEventEnvelope>>(&payload) {
                Ok(parsed) => parsed,
                Err(err) => {
                    warn!(cache_key = %cache_key, error = %err, "invalid idempotent stream payload");
                    return None;
                }
            };
            let mut cache = self.idempotent_streams.lock().await;
            cache.insert(cache_key.to_owned(), cached.clone());
            return Some(cached);
        }

        let cache = self.idempotent_streams.lock().await;
        cache.get(cache_key).cloned()
    }

    async fn store_cached_stream_events(&self, cache_key: &str, events: &[StreamEventEnvelope]) {
        if self.persistence.is_none() {
            let mut cache = self.idempotent_streams.lock().await;
            if cache.len() >= MAX_IDEMPOTENT_CACHE_ENTRIES && !cache.contains_key(cache_key) {
                return;
            }
            cache.insert(cache_key.to_owned(), events.to_vec());
            return;
        }
        let Some(repo) = self.persistence_backend().await else {
            warn!(
                cache_key = %cache_key,
                "idempotency backend unavailable while persistence is enabled"
            );
            return;
        };
        let payload = match serde_json::to_string(events) {
            Ok(payload) => payload,
            Err(err) => {
                warn!(cache_key = %cache_key, error = %err, "failed to serialize idempotent stream cache");
                return;
            }
        };
        if let Err(err) = repo
            .store_idempotency_payload(cache_key, &payload, STREAM_IDEMPOTENCY_TTL_SECS)
            .await
        {
            warn!(cache_key = %cache_key, error = %err, "failed to persist idempotent stream cache");
            return;
        }
        let mut cache = self.idempotent_streams.lock().await;
        cache.insert(cache_key.to_owned(), events.to_vec());
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
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        ensure_session_branches(session);
        let branch_id = session.active_branch_id.clone();
        session.messages.push(message.clone());
        sync_active_branch_from_session(session);
        drop(sessions);
        self.persist_session_message(session_id, &branch_id, &message)
            .await;

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
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
        ensure_session_branches(session);
        let branch_id = session.active_branch_id.clone();
        session.events.push(event.clone());
        sync_active_branch_from_session(session);
        drop(sessions);
        self.persist_session_event(session_id, &branch_id, &event)
            .await;
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
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
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
        let mut sessions = self.sessions.write().await;
        let (active_branch_id, branch_artifact_ids) = {
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            })?;
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
        let artifact_snapshot = artifacts.get(&artifact_id).cloned();
        drop(artifacts);

        {
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                PipelineError::NotFound(format!("session '{}' not found", session_id))
            })?;
            ensure_session_branches(session);
            if let Some(previous_id) = replaced_artifact_id {
                session.artifacts.remove(&previous_id);
            }
            session.artifacts.insert(artifact_id.clone());
            sync_active_branch_from_session(session);
        }
        drop(sessions);
        if let Some(record) = artifact_snapshot.as_ref() {
            self.persist_artifact_record(record).await;
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
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            PipelineError::NotFound(format!("session '{}' not found", session_id))
        })?;
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
        let job = WorkflowJob {
            workflow_id: workflow_id.to_owned(),
            session_id: session_id.to_owned(),
            step: step.to_owned(),
            payload: payload.to_string(),
            timestamp_ms: now,
            attempt: 0,
            durable_entry_id: None,
        };
        if !self.enqueue_workflow_durable(&job).await {
            let mut queue = self.workflow_queue.lock().await;
            queue.push_back(job);
        }

        let workflow_record = {
            let mut workflows = self.workflows.write().await;
            let record = WorkflowRecord {
                workflow_id: workflow_id.to_owned(),
                session_id: session_id.to_owned(),
                status: "active".to_owned(),
                current_step: step.to_owned(),
                updated_at: now,
                callback_url: None,
            };
            workflows.insert(workflow_id.to_owned(), record.clone());
            record
        };
        self.persist_workflow_record(&workflow_record).await;
    }

    async fn bind_session_workflow(&self, session_id: &str, workflow_id: &str) {
        let mut sessions = self.sessions.write().await;
        let Some(session) = sessions.get_mut(session_id) else {
            warn!(
                session_id = %session_id,
                workflow_id = %workflow_id,
                "cannot bind workflow to missing session"
            );
            return;
        };
        session.workflow_id = Some(workflow_id.to_owned());
        let snapshot = session.clone();
        drop(sessions);
        self.persist_session_meta_record(&snapshot).await;
    }

    async fn next_workflow_job(&self) -> (Option<WorkflowJob>, WorkflowJobSource) {
        if let Some(repo) = self.persistence_backend().await {
            match repo.dequeue_workflow_job().await {
                Ok(Some((entry_id, job))) => {
                    return (
                        Some(WorkflowJob {
                            workflow_id: job.workflow_id,
                            session_id: job.session_id,
                            step: job.step,
                            payload: job.payload,
                            timestamp_ms: job.timestamp_ms,
                            attempt: job.attempt,
                            durable_entry_id: Some(entry_id),
                        }),
                        WorkflowJobSource::Durable,
                    );
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(error = %err, "failed to dequeue durable workflow job");
                }
            }
        }

        let mut queue = self.workflow_queue.lock().await;
        let next = queue.pop_front();
        (next, WorkflowJobSource::InMemory)
    }

    async fn requeue_workflow_job(&self, mut job: WorkflowJob, source: WorkflowJobSource) {
        self.ack_workflow_job_if_needed(&job, source).await;
        job.attempt = job.attempt.saturating_add(1);
        job.durable_entry_id = None;
        if matches!(source, WorkflowJobSource::Durable) {
            if self.enqueue_workflow_durable(&job).await {
                return;
            }
            warn!(
                workflow_id = %job.workflow_id,
                session_id = %job.session_id,
                "durable workflow requeue failed, falling back to in-memory queue"
            );
        }
        let mut queue = self.workflow_queue.lock().await;
        queue.push_back(job);
    }

    async fn enqueue_workflow_durable(&self, job: &WorkflowJob) -> bool {
        let Some(repo) = self.persistence_backend().await else {
            return false;
        };
        let durable = DurableWorkflowJob {
            workflow_id: job.workflow_id.clone(),
            session_id: job.session_id.clone(),
            step: job.step.clone(),
            payload: job.payload.clone(),
            timestamp_ms: job.timestamp_ms,
            attempt: job.attempt,
        };
        match repo.enqueue_workflow_job(&durable).await {
            Ok(()) => true,
            Err(err) => {
                warn!(error = %err, "failed to enqueue workflow job durably");
                false
            }
        }
    }

    async fn next_notion_sync_job(&self) -> (Option<NotionSyncJob>, WorkflowJobSource) {
        if let Some(repo) = self.persistence_backend().await {
            match repo.dequeue_notion_sync_job().await {
                Ok(Some((entry_id, job))) => {
                    return (
                        Some(NotionSyncJob {
                            session_id: job.session_id,
                            summary: job.summary,
                            artifacts: job.artifacts,
                            timestamp_ms: job.timestamp_ms,
                            attempt: job.attempt,
                            durable_entry_id: Some(entry_id),
                        }),
                        WorkflowJobSource::Durable,
                    );
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(error = %err, "failed to dequeue durable notion sync job");
                }
            }
        }
        let mut queue = self.notion_queue.lock().await;
        let next = queue.pop_front();
        (next, WorkflowJobSource::InMemory)
    }

    async fn enqueue_notion_sync_durable(&self, job: &NotionSyncJob) -> bool {
        let Some(repo) = self.persistence_backend().await else {
            return false;
        };
        let durable = DurableNotionSyncJob {
            session_id: job.session_id.clone(),
            summary: job.summary.clone(),
            artifacts: job.artifacts.clone(),
            timestamp_ms: job.timestamp_ms,
            attempt: job.attempt,
        };
        match repo.enqueue_notion_sync_job(&durable).await {
            Ok(()) => true,
            Err(err) => {
                warn!(error = %err, "failed to enqueue notion sync job durably");
                false
            }
        }
    }

    async fn ack_workflow_job_if_needed(&self, job: &WorkflowJob, source: WorkflowJobSource) {
        if !matches!(source, WorkflowJobSource::Durable) {
            return;
        }
        let Some(entry_id) = job.durable_entry_id.as_deref() else {
            return;
        };
        let Some(repo) = self.persistence_backend().await else {
            warn!(
                workflow_id = %job.workflow_id,
                entry_id = %entry_id,
                "persistence backend unavailable for workflow ack"
            );
            return;
        };
        if let Err(err) = repo.ack_workflow_job_entry(entry_id).await {
            warn!(
                workflow_id = %job.workflow_id,
                entry_id = %entry_id,
                error = %err,
                "failed to ack workflow queue entry"
            );
        }
    }

    async fn ack_notion_job_if_needed(&self, job: &NotionSyncJob, source: WorkflowJobSource) {
        if !matches!(source, WorkflowJobSource::Durable) {
            return;
        }
        let Some(entry_id) = job.durable_entry_id.as_deref() else {
            return;
        };
        let Some(repo) = self.persistence_backend().await else {
            warn!(
                session_id = %job.session_id,
                entry_id = %entry_id,
                "persistence backend unavailable for notion ack"
            );
            return;
        };
        if let Err(err) = repo.ack_notion_sync_job_entry(entry_id).await {
            warn!(
                session_id = %job.session_id,
                entry_id = %entry_id,
                error = %err,
                "failed to ack notion queue entry"
            );
        }
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
        logs.entry(session_id.to_owned())
            .or_default()
            .push(entry.clone());
        drop(logs);

        if let Some(repo) = self.persistence_backend().await {
            let persisted = PersistedAgentLogEntry {
                ts: entry.ts,
                selected_agent: entry.selected_agent,
                reason: entry.reason,
            };
            if let Err(err) = repo.append_agent_log(session_id, &persisted).await {
                warn!(
                    session_id = %session_id,
                    error = %err,
                    "failed to persist agent log entry"
                );
            }
        }
    }

    async fn enqueue_notion_sync(
        &self,
        session_id: &str,
        summary: &str,
        artifacts: &[ArtifactEnvelope],
    ) -> bool {
        if !self.notion_sync_config.is_active() {
            info!(
                session_id = %session_id,
                "skipping notion sync enqueue because integration is inactive"
            );
            return false;
        }

        let now = now_ms();
        let job = NotionSyncJob {
            session_id: session_id.to_owned(),
            summary: summary.to_owned(),
            artifacts: artifacts
                .iter()
                .map(|artifact| artifact.artifact_id.clone())
                .collect::<Vec<_>>(),
            timestamp_ms: now,
            attempt: 0,
            durable_entry_id: None,
        };
        if self.enqueue_notion_sync_durable(&job).await {
            info!(session_id = %session_id, "notion sync durably enqueued");
            return true;
        }
        let mut queue = self.notion_queue.lock().await;
        queue.push_back(job);
        info!(session_id = %session_id, "notion sync enqueued in-memory");
        true
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

fn map_message_mode(mode: &MessageMode) -> ClientAgentMode {
    match mode {
        MessageMode::Chat => ClientAgentMode::Chat,
        MessageMode::Workflow => ClientAgentMode::Workflow,
        MessageMode::Execution => ClientAgentMode::Execution,
    }
}

fn map_agent_kind(agent: AgentKind) -> AgentProvider {
    match agent {
        AgentKind::Claude => AgentProvider::Claude,
        AgentKind::DeepSeek => AgentProvider::DeepSeek,
    }
}

fn map_agent_error(error: AgentError) -> PipelineError {
    match error {
        AgentError::Configuration(message) => PipelineError::InvalidInput(message),
        AgentError::ProviderUnavailable(message) => PipelineError::Internal(message),
        AgentError::Timeout { stage } => {
            PipelineError::Internal(format!("agent provider timeout during {stage}"))
        }
        AgentError::Transport(message) => PipelineError::Internal(message),
        AgentError::HttpStatus { status, body } => {
            PipelineError::Internal(format!("agent provider http status {status}: {body}"))
        }
        AgentError::RateLimited { retry_after_ms } => {
            PipelineError::Internal(match retry_after_ms {
                Some(ms) => format!("agent provider rate limited, retry after {ms}ms"),
                None => "agent provider rate limited".to_owned(),
            })
        }
        AgentError::Deserialize(message) => PipelineError::Internal(message),
        AgentError::StreamTerminated => {
            PipelineError::Internal("agent provider stream terminated".to_owned())
        }
        AgentError::NonRetryable(message) => PipelineError::Internal(message),
    }
}

fn build_local_agent_response(
    input: &str,
    context: &SessionContext,
    mode: &MessageMode,
    agent: AgentKind,
) -> String {
    let agent_name = match agent {
        AgentKind::Claude => "claude-local",
        AgentKind::DeepSeek => "deepseek-local",
    };
    let goal = context.state.goal.clone().or_else(|| match mode {
        MessageMode::Workflow => Some("complete workflow objective".to_owned()),
        MessageMode::Execution => Some("execute requested operation".to_owned()),
        MessageMode::Chat => Some("provide useful response".to_owned()),
    });
    let step = Some(match mode {
        MessageMode::Workflow => "workflow-progress".to_owned(),
        MessageMode::Execution => "execution-progress".to_owned(),
        MessageMode::Chat => "continue".to_owned(),
    });
    let memory_hint = context
        .memory
        .last()
        .map(|message| truncate_for_notion(&message.content, 240))
        .unwrap_or_else(|| "no prior context".to_owned());
    let response =
        format!("({agent_name}) processed request: {input}. Context hint: {memory_hint}");
    let artifact = if matches!(mode, MessageMode::Execution)
        || input.to_ascii_lowercase().contains("code")
        || input.to_ascii_lowercase().contains("artifact")
    {
        Some(serde_json::json!({
            "type": "code",
            "content": format!("// local-runtime artifact\n// request: {input}\n"),
        }))
    } else {
        None
    };

    let payload = serde_json::json!({
        "response": response,
        "state": {
            "goal": goal,
            "step": step,
        },
        "artifact": artifact,
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| input.to_owned())
}

fn local_stream_chunks(text: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        current.push(ch);
        if current.chars().count() >= 32 || ch.is_whitespace() {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() && !text.is_empty() {
        chunks.push(text.to_owned());
    }
    chunks
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

fn build_agent_output_from_text(
    input: &str,
    context: &SessionContext,
    mode: &MessageMode,
    response_text: String,
) -> AgentOutput {
    let structured = parse_structured_agent_response(&response_text);
    let lower = input.to_ascii_lowercase();
    let (structured_text, structured_goal, structured_step, structured_artifact) =
        if let Some(structured) = structured {
            let StructuredAgentResponse {
                response,
                step,
                state,
                artifact,
            } = structured;
            let (goal_from_state, step_from_state) = if let Some(state) = state {
                (state.goal, state.step)
            } else {
                (None, None)
            };
            (
                response,
                goal_from_state,
                step_from_state.or(step),
                artifact,
            )
        } else {
            (None, None, None, None)
        };

    let proposed_goal = structured_goal.or_else(|| {
        if lower.contains("finalize") {
            Some("finalize landing page".to_owned())
        } else if lower.contains("draft") {
            Some("draft landing page".to_owned())
        } else {
            context.state.goal.clone()
        }
    });

    let proposed_step = structured_step.or_else(|| {
        if lower.contains("refine") || lower.contains("finalize") {
            Some("refine".to_owned())
        } else if lower.contains("draft") {
            Some("draft".to_owned())
        } else {
            Some("continue".to_owned())
        }
    });

    let (artifact_type, artifact_content) = if let Some(artifact) = structured_artifact {
        (
            Some(artifact.artifact_type.unwrap_or_else(|| "code".to_owned())),
            artifact
                .content
                .map(|content| truncate_for_notion(&content, 8_000))
                .or_else(|| Some(render_artifact_content("code", &response_text))),
        )
    } else if lower.contains("artifact")
        || lower.contains("code")
        || matches!(mode, MessageMode::Execution)
    {
        let rendered = render_artifact_content("code", &response_text);
        (Some("code".to_owned()), Some(rendered))
    } else {
        (None, None)
    };

    AgentOutput {
        text: structured_text
            .filter(|text| !text.trim().is_empty())
            .unwrap_or(response_text),
        proposed_goal,
        proposed_step,
        artifact_type,
        artifact_content,
    }
}

fn render_artifact_content(artifact_type: &str, source: &str) -> String {
    if let Some(code) = extract_fenced_block(source) {
        return code;
    }
    if source.trim().is_empty() {
        return format!("artifact:{artifact_type}");
    }
    source.trim().to_owned()
}

#[derive(Debug, Deserialize)]
struct StructuredAgentResponse {
    #[serde(default)]
    response: Option<String>,
    #[serde(default)]
    step: Option<String>,
    #[serde(default)]
    state: Option<StructuredAgentState>,
    #[serde(default)]
    artifact: Option<StructuredAgentArtifact>,
}

#[derive(Debug, Deserialize)]
struct StructuredAgentState {
    #[serde(default)]
    goal: Option<String>,
    #[serde(default)]
    step: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StructuredAgentArtifact {
    #[serde(default, rename = "type")]
    artifact_type: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

fn parse_structured_agent_response(raw: &str) -> Option<StructuredAgentResponse> {
    for candidate in structured_json_candidates(raw) {
        if let Ok(parsed) = serde_json::from_str::<StructuredAgentResponse>(&candidate) {
            return Some(parsed);
        }
    }
    None
}

fn structured_json_candidates(raw: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    let trimmed = raw.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        candidates.push(trimmed.to_owned());
    }
    if let Some(block) = extract_tagged_fenced_block(trimmed, "json") {
        candidates.push(block);
    }
    if let Some(block) = extract_fenced_block(trimmed) {
        let block_trimmed = block.trim();
        if block_trimmed.starts_with('{') && block_trimmed.ends_with('}') {
            candidates.push(block_trimmed.to_owned());
        }
    }
    candidates
}

fn extract_tagged_fenced_block(input: &str, tag: &str) -> Option<String> {
    let open = format!("```{tag}");
    let start = input.find(&open)?;
    let after_open = &input[start + open.len()..];
    let end = after_open.find("```")?;
    Some(after_open[..end].trim().to_owned())
}

fn extract_fenced_block(input: &str) -> Option<String> {
    let start = input.find("```")?;
    let after_open = &input[start + 3..];
    let end = after_open.find("```")?;
    Some(after_open[..end].trim().to_owned())
}

fn apply_state_mutation(
    prev: &SessionState,
    user_input: &str,
    output: &AgentOutput,
) -> SessionState {
    let mut next = prev.clone();
    next.version = prev.version.saturating_add(1);
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

fn read_string_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_bool_env(key: &str, default: bool) -> bool {
    let Some(raw) = std::env::var(key).ok() else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn read_u64_env(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn read_u32_env(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

fn notion_title_property(title: &str) -> Value {
    serde_json::json!({
        "title": [{
            "type": "text",
            "text": {
                "content": truncate_for_notion(title, 1800)
            }
        }]
    })
}

fn notion_paragraph_block(content: &str) -> Value {
    serde_json::json!({
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{
                "type": "text",
                "text": {
                    "content": truncate_for_notion(content, 1800)
                }
            }]
        }
    })
}

fn truncate_for_notion(input: &str, max_chars: usize) -> String {
    input.chars().take(max_chars).collect::<String>()
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
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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
