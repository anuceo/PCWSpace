use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleMessageRequest {
    pub content: String,
    #[serde(default)]
    pub workflow_active: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactSummary {
    pub artifact_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HandleMessageResponse {
    pub message: String,
    pub artifacts: Vec<ArtifactSummary>,
    pub deltashot_id: String,
    pub state_version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageAcceptedResponse {
    pub ok: bool,
    pub result: Option<HandleMessageResponse>,
    pub error: Option<String>,
}

impl MessageAcceptedResponse {
    pub fn success(result: HandleMessageResponse) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(error: String) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionView {
    pub session_id: String,
    pub status: String,
    pub workflow_id: Option<String>,
    pub state_version: u64,
    pub message_count: usize,
    pub event_count: usize,
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("session lock unavailable")]
    LockUnavailable,
    #[error("internal serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone)]
struct SessionContext {
    session_id: String,
    state: SessionState,
    memory: Vec<ChatMessage>,
    active_workflow: Option<String>,
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
struct SessionMeta {
    created_at_ms: u64,
    status: String,
    workflow_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    meta: SessionMeta,
    state: SessionState,
    messages: Vec<ChatMessage>,
    events: Vec<SessionEvent>,
    hashchain: Vec<String>,
    artifacts: HashSet<String>,
}

impl SessionRecord {
    fn new() -> Self {
        Self {
            meta: SessionMeta {
                created_at_ms: now_ms(),
                status: "active".to_owned(),
                workflow_id: None,
            },
            state: SessionState::default(),
            messages: Vec::new(),
            events: Vec::new(),
            hashchain: Vec::new(),
            artifacts: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    id: String,
    role: String,
    content: String,
    timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEvent {
    id: String,
    event_type: String,
    payload: Value,
    timestamp_ms: u64,
}

#[derive(Debug, Clone)]
struct ArtifactRecord {
    artifact_id: String,
    session_id: String,
    current_version: u64,
    versions: BTreeMap<u64, String>,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltashotRecord {
    id: String,
    session_id: String,
    timestamp_ms: u64,
    hash: String,
    prev_hash: Option<String>,
    event_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct WorkflowJob {
    workflow_id: String,
    session_id: String,
    step: String,
    payload: String,
}

#[derive(Debug, Clone)]
struct NotionSyncJob {
    session_id: String,
    summary: String,
    artifacts: Vec<String>,
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
    artifact: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateDiff {
    event_type: String,
    changes: BTreeMap<String, DiffChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiffChange {
    from: String,
    to: String,
}

#[derive(Debug)]
pub struct ExecutionPipeline {
    keyspace: RedisKeyspace,
    id_counter: AtomicU64,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    artifacts: RwLock<HashMap<String, ArtifactRecord>>,
    workflow_queue: Mutex<Vec<WorkflowJob>>,
    notion_queue: Arc<Mutex<Vec<NotionSyncJob>>>,
    locks: Mutex<HashMap<String, Instant>>,
}

impl ExecutionPipeline {
    pub fn new(env: &str) -> Result<Self, PipelineError> {
        let keyspace = RedisKeyspace::new(env)
            .map_err(|err| PipelineError::InvalidInput(format!("invalid env segment: {err}")))?;
        validate_spec_integrity()
            .map_err(|err| PipelineError::InvalidInput(format!("invalid key spec: {err}")))?;

        Ok(Self {
            keyspace,
            id_counter: AtomicU64::new(1),
            sessions: RwLock::new(HashMap::new()),
            artifacts: RwLock::new(HashMap::new()),
            workflow_queue: Mutex::new(Vec::new()),
            notion_queue: Arc::new(Mutex::new(Vec::new())),
            locks: Mutex::new(HashMap::new()),
        })
    }

    pub async fn handle_message(
        &self,
        session_id: &str,
        request: HandleMessageRequest,
    ) -> Result<HandleMessageResponse, PipelineError> {
        if request.content.trim().is_empty() {
            return Err(PipelineError::InvalidInput(
                "message content cannot be empty".to_owned(),
            ));
        }

        // 2) Session hydration
        let context = self.hydrate_context(session_id).await?;
        info!(
            session_id = %context.session_id,
            state_key = %self
                .keyspace
                .session_state(session_id)
                .unwrap_or_else(|_| "<invalid>".to_owned()),
            messages_key = %self
                .keyspace
                .session_messages(session_id)
                .unwrap_or_else(|_| "<invalid>".to_owned()),
            "session hydrated"
        );

        // 3) Lock acquisition
        let lock_key = self.acquire_session_lock(session_id).await?;
        info!(session_id = %session_id, lock_key = %lock_key, "session lock acquired");

        let process_result = self.run_locked_pipeline(session_id, request, context).await;

        self.release_lock(&lock_key).await;
        info!(session_id = %session_id, lock_key = %lock_key, "session lock released");

        process_result
    }

    async fn run_locked_pipeline(
        &self,
        session_id: &str,
        request: HandleMessageRequest,
        context: SessionContext,
    ) -> Result<HandleMessageResponse, PipelineError> {
        // 4) Append user message first (event-first design)
        let user_message = self
            .append_message(session_id, "user", &request.content)
            .await?;
        info!(
            session_id = %session_id,
            message_stream = %self
                .keyspace
                .session_messages(session_id)
                .unwrap_or_else(|_| "<invalid>".to_owned()),
            message_id = %user_message.id,
            "user message appended"
        );

        // 5) Agent routing
        let selected_agent = select_agent(&request.content);
        info!(session_id = %session_id, agent = ?selected_agent, "agent selected");

        // 6) Agent execution
        let agent_output = run_agent(selected_agent, &request.content, &context);

        // 7) State mutation + diff
        let prev_state = context.state.clone();
        let next_state = apply_state_mutation(&prev_state, &request.content, &agent_output);
        let diff = compute_diff(&prev_state, &next_state);

        // 8) DeltaShot creation + session event logging + hash chain update
        let state_event = self
            .append_event(
                session_id,
                EVENT_TYPE_STATE_UPDATE,
                serde_json::to_value(&diff)
                    .map_err(|err| PipelineError::Serialization(err.to_string()))?,
            )
            .await?;
        let deltashot = self.create_deltashot(session_id, &state_event.id).await?;
        info!(
            session_id = %session_id,
            events_key = %self
                .keyspace
                .session_events(session_id)
                .unwrap_or_else(|_| "<invalid>".to_owned()),
            deltashot_id = %deltashot.id,
            "deltashot created"
        );

        // 9) Artifact handling
        let artifacts = if let Some(content) = &agent_output.artifact {
            let artifact = self.create_or_update_artifact(session_id, content).await?;

            let artifact_event_payload = serde_json::json!({
                "artifact_id": artifact.artifact_id,
                "version": artifact.version,
            });
            let _ = self
                .append_event(
                    session_id,
                    EVENT_TYPE_ARTIFACT_UPDATE,
                    artifact_event_payload,
                )
                .await?;

            vec![artifact]
        } else {
            Vec::new()
        };

        // 10) Append agent response
        let _ = self
            .append_message(session_id, "agent", &agent_output.text)
            .await?;
        let _ = self
            .append_event(
                session_id,
                EVENT_TYPE_MESSAGE_APPEND,
                serde_json::json!({
                    "role": "agent",
                    "content": agent_output.text,
                }),
            )
            .await?;

        // 11) Persist updated state
        self.persist_state(session_id, next_state.clone()).await?;

        // 12) Workflow continuation
        if request.workflow_active || context.active_workflow.is_some() {
            self.enqueue_workflow(
                context
                    .active_workflow
                    .as_deref()
                    .unwrap_or("workflow-default"),
                session_id,
                next_state.step.as_deref().unwrap_or("continue"),
            )
            .await;
        }

        // 13) Async notion sync (never blocks response path)
        self.enqueue_notion_sync(session_id, &agent_output.text, &artifacts)
            .await;

        Ok(HandleMessageResponse {
            message: agent_output.text,
            artifacts,
            deltashot_id: deltashot.id,
            state_version: next_state.version,
        })
    }

    async fn hydrate_context(&self, session_id: &str) -> Result<SessionContext, PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);

        let mut recent = session.messages.clone();
        recent.reverse();
        recent.truncate(RECENT_MESSAGE_WINDOW);

        Ok(SessionContext {
            session_id: session_id.to_owned(),
            state: session.state.clone(),
            memory: recent,
            active_workflow: session.meta.workflow_id.clone(),
        })
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
        let msg = ChatMessage {
            id: self.next_id("msg"),
            role: role.to_owned(),
            content: content.to_owned(),
            timestamp_ms: now_ms(),
        };

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);
        session.messages.push(msg.clone());

        Ok(msg)
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
            timestamp_ms: now_ms(),
        };

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);
        session.events.push(event.clone());
        Ok(event)
    }

    async fn create_deltashot(
        &self,
        session_id: &str,
        state_event_id: &str,
    ) -> Result<DeltashotRecord, PipelineError> {
        let timestamp_ms = now_ms();
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);

        let prev_hash = session.hashchain.last().cloned();
        let id = self.next_id("ds");
        let digest_input = serde_json::json!({
            "id": id,
            "session_id": session_id,
            "timestamp_ms": timestamp_ms,
            "prev_hash": prev_hash,
            "event_ids": [state_event_id],
        });
        let encoded = serde_json::to_vec(&digest_input)
            .map_err(|err| PipelineError::Serialization(err.to_string()))?;
        let hash = blake3::hash(&encoded).to_hex().to_string();

        let record = DeltashotRecord {
            id,
            session_id: session_id.to_owned(),
            timestamp_ms,
            hash: hash.clone(),
            prev_hash,
            event_ids: vec![state_event_id.to_owned()],
        };

        session.hashchain.push(hash);
        Ok(record)
    }

    async fn create_or_update_artifact(
        &self,
        session_id: &str,
        content: &str,
    ) -> Result<ArtifactSummary, PipelineError> {
        let artifact_id = format!("artifact-{}", session_id);
        let mut artifacts = self.artifacts.write().await;
        let record = artifacts
            .entry(artifact_id.clone())
            .or_insert_with(|| ArtifactRecord {
                artifact_id: artifact_id.clone(),
                session_id: session_id.to_owned(),
                current_version: 0,
                versions: BTreeMap::new(),
                created_at_ms: now_ms(),
            });
        record.current_version += 1;
        record
            .versions
            .insert(record.current_version, content.to_owned());

        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .entry(session_id.to_owned())
                .or_insert_with(SessionRecord::new);
            session.artifacts.insert(artifact_id.clone());
        }

        Ok(ArtifactSummary {
            artifact_id,
            version: record.current_version,
        })
    }

    async fn persist_state(
        &self,
        session_id: &str,
        next_state: SessionState,
    ) -> Result<(), PipelineError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);
        session.state = next_state;
        Ok(())
    }

    async fn enqueue_workflow(&self, workflow_id: &str, session_id: &str, next_step: &str) {
        let job = WorkflowJob {
            workflow_id: workflow_id.to_owned(),
            session_id: session_id.to_owned(),
            step: next_step.to_owned(),
            payload: "auto-continued".to_owned(),
        };
        let mut queue = self.workflow_queue.lock().await;
        queue.push(job);
        info!(session_id = %session_id, workflow_id = %workflow_id, "workflow enqueued");
    }

    async fn enqueue_notion_sync(
        &self,
        session_id: &str,
        summary: &str,
        artifacts: &[ArtifactSummary],
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
            let mut queue = queue.lock().await;
            queue.push(job);
            info!(session_id = %session_id, "notion sync enqueued");
        });
    }

    fn next_id(&self, prefix: &str) -> String {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{}", n)
    }

    pub async fn get_or_create_session_view(&self, session_id: &str) -> SessionView {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_owned())
            .or_insert_with(SessionRecord::new);

        SessionView {
            session_id: session_id.to_owned(),
            status: session.meta.status.clone(),
            workflow_id: session.meta.workflow_id.clone(),
            state_version: session.state.version,
            message_count: session.messages.len(),
            event_count: session.events.len(),
        }
    }

    #[cfg(test)]
    async fn replay_state_version(&self, session_id: &str) -> Option<u64> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        let state_updates = session
            .events
            .iter()
            .filter(|event| event.event_type == EVENT_TYPE_STATE_UPDATE)
            .count() as u64;
        Some(state_updates)
    }
}

pub fn shared_pipeline() -> &'static ExecutionPipeline {
    PIPELINE.get_or_init(|| {
        let env = std::env::var("DELTA_AGENT_ENV").unwrap_or_else(|_| "dev".to_owned());
        ExecutionPipeline::new(&env).expect("shared pipeline initialization must succeed")
    })
}

pub type PipelineState = ExecutionPipeline;

fn select_agent(input: &str) -> AgentKind {
    let lower = input.to_ascii_lowercase();
    if lower.contains("code")
        || lower.contains("rust")
        || lower.contains("implement")
        || lower.contains("compile")
    {
        AgentKind::DeepSeek
    } else {
        AgentKind::Claude
    }
}

fn run_agent(agent: AgentKind, input: &str, context: &SessionContext) -> AgentOutput {
    let complexity = estimate_complexity(input);
    let memory_hint = context.memory.len();
    let prefix = match agent {
        AgentKind::Claude => "reasoning",
        AgentKind::DeepSeek => "technical",
    };

    let response_text = format!(
        "[{prefix}:{complexity}] processed '{}' with {} memory items",
        input, memory_hint
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

    let artifact = if lower.contains("artifact") || lower.contains("code") {
        Some(format!("artifact payload for '{}'", input))
    } else {
        None
    };

    AgentOutput {
        text: response_text,
        proposed_goal,
        proposed_step,
        artifact,
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

fn compute_diff(prev: &SessionState, next: &SessionState) -> StateDiff {
    let mut changes = BTreeMap::new();
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

    StateDiff {
        event_type: EVENT_TYPE_STATE_UPDATE.to_owned(),
        changes,
    }
}

fn maybe_insert_change(
    changes: &mut BTreeMap<String, DiffChange>,
    field: &str,
    from: String,
    to: String,
) {
    if from != to {
        changes.insert(field.to_owned(), DiffChange { from, to });
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
        let response = pipeline
            .handle_message(
                "session-1",
                HandleMessageRequest {
                    content: "draft artifact and refine".to_owned(),
                    workflow_active: true,
                },
            )
            .await
            .expect("pipeline should succeed");

        assert_eq!(response.state_version, 1);
        assert_eq!(response.artifacts.len(), 1);

        let replay_version = pipeline
            .replay_state_version("session-1")
            .await
            .expect("session should exist");
        assert_eq!(replay_version, response.state_version);
    }

    #[test]
    fn agent_routing_picks_technical_for_code_intent() {
        assert!(matches!(
            select_agent("please implement rust code"),
            AgentKind::DeepSeek
        ));
        assert!(matches!(select_agent("help me reason"), AgentKind::Claude));
    }
}
