use std::collections::HashSet;

use crate::errors::CoreError;

pub const WORKFLOW_WORKERS_GROUP: &str = "workflow_workers";
pub const LOCK_TTL_MIN_SECS: u64 = 5;
pub const LOCK_TTL_MAX_SECS: u64 = 30;
pub const SNAPSHOT_INTERVAL_EVENTS: u64 = 100;

pub const EVENT_TYPE_STATE_UPDATE: &str = "STATE_UPDATE";
pub const EVENT_TYPE_ARTIFACT_UPDATE: &str = "ARTIFACT_UPDATE";
pub const EVENT_TYPE_MESSAGE_APPEND: &str = "MESSAGE_APPEND";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedisPrimitive {
    Hash,
    Json,
    Stream,
    List,
    Set,
    ZSet,
    String,
    PubSubChannel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetentionClass {
    Persistent,
    ShortLived,
    Lock,
    Ephemeral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySpec {
    pub key_name: &'static str,
    pub template: &'static str,
    pub primitive: RedisPrimitive,
    pub retention: RetentionClass,
    pub notes: &'static str,
}

const KEY_SPECS: &[KeySpec] = &[
    KeySpec {
        key_name: "workspace_meta",
        template: "{env}:workspace:{workspaceId}:meta",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Workspace metadata state",
    },
    KeySpec {
        key_name: "workspace_sessions",
        template: "{env}:workspace:{workspaceId}:sessions",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Workspace sessions ordered by timestamp",
    },
    KeySpec {
        key_name: "session_meta",
        template: "{env}:session:{sessionId}:meta",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Session metadata",
    },
    KeySpec {
        key_name: "session_state",
        template: "{env}:session:{sessionId}:state",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::Persistent,
        notes: "Hot session state snapshot",
    },
    KeySpec {
        key_name: "session_messages",
        template: "{env}:session:{sessionId}:messages",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Session chat log stream",
    },
    KeySpec {
        key_name: "session_deltashots",
        template: "{env}:session:{sessionId}:deltashots",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "DeltaShot index by timestamp",
    },
    KeySpec {
        key_name: "deltashots_stream",
        template: "{env}:deltashots:stream",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Global DeltaShot stream (optional)",
    },
    KeySpec {
        key_name: "session_events",
        template: "{env}:session:{sessionId}:events",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Primary per-session event stream",
    },
    KeySpec {
        key_name: "deltashot_object",
        template: "{env}:deltashot:{deltashotId}",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "DeltaShot object store",
    },
    KeySpec {
        key_name: "session_hashchain",
        template: "{env}:session:{sessionId}:hashchain",
        primitive: RedisPrimitive::List,
        retention: RetentionClass::Persistent,
        notes: "Session-level hash chain",
    },
    KeySpec {
        key_name: "artifact_meta",
        template: "{env}:artifact:{artifactId}:meta",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Artifact metadata",
    },
    KeySpec {
        key_name: "artifact_versions",
        template: "{env}:artifact:{artifactId}:versions",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Artifact version index",
    },
    KeySpec {
        key_name: "artifact_version_content",
        template: "{env}:artifact:{artifactId}:version:{versionId}",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::Persistent,
        notes: "Artifact version payload (STRING/JSON)",
    },
    KeySpec {
        key_name: "session_artifacts",
        template: "{env}:session:{sessionId}:artifacts",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Artifact linkage for session",
    },
    KeySpec {
        key_name: "workflow_state",
        template: "{env}:workflow:{workflowId}:state",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Workflow state store",
    },
    KeySpec {
        key_name: "workflow_queue",
        template: "{env}:queue:workflow",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Workflow queue stream",
    },
    KeySpec {
        key_name: "notion_sync_queue",
        template: "{env}:queue:notion_sync",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Notion sync queue stream",
    },
    KeySpec {
        key_name: "agent_queue",
        template: "{env}:queue:agent",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Agent task queue stream",
    },
    KeySpec {
        key_name: "session_agent_responses",
        template: "{env}:session:{sessionId}:agent_responses",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Agent response stream",
    },
    KeySpec {
        key_name: "session_agent_context",
        template: "{env}:session:{sessionId}:agent_context",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::ShortLived,
        notes: "Short-lived agent context cache",
    },
    KeySpec {
        key_name: "lock_session",
        template: "{env}:lock:session:{sessionId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Session lock with TTL",
    },
    KeySpec {
        key_name: "lock_artifact",
        template: "{env}:lock:artifact:{artifactId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Artifact lock with TTL",
    },
    KeySpec {
        key_name: "lock_workflow",
        template: "{env}:lock:workflow:{workflowId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Workflow lock with TTL",
    },
    KeySpec {
        key_name: "sessions_recent",
        template: "{env}:sessions:recent",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Recent global sessions",
    },
    KeySpec {
        key_name: "sessions_active",
        template: "{env}:sessions:active",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Active sessions set",
    },
    KeySpec {
        key_name: "artifacts_hot",
        template: "{env}:artifacts:hot",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Hot artifacts index",
    },
    KeySpec {
        key_name: "session_branches",
        template: "{env}:session:{sessionId}:branches",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Session branches index",
    },
    KeySpec {
        key_name: "session_active_branch",
        template: "{env}:session:{sessionId}:active_branch",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Persistent,
        notes: "Active branch pointer for session",
    },
    KeySpec {
        key_name: "branch_events",
        template: "{env}:branch:{branchId}:events",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Per-branch event stream",
    },
    KeySpec {
        key_name: "branch_state",
        template: "{env}:branch:{branchId}:state",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::Persistent,
        notes: "Per-branch state snapshot",
    },
    KeySpec {
        key_name: "session_snapshot",
        template: "{env}:session:{sessionId}:snapshot:{n}",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::Persistent,
        notes: "Periodic full state snapshots",
    },
    KeySpec {
        key_name: "branch_meta",
        template: "{env}:branch:{branchId}:meta",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Branch metadata including Penpal fields",
    },
    KeySpec {
        key_name: "branch_deltashots",
        template: "{env}:branch:{branchId}:deltashots",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Branch DeltaShot index",
    },
    KeySpec {
        key_name: "branch_active_ds",
        template: "{env}:branch:{branchId}:active_ds",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Persistent,
        notes: "Branch active DeltaShot pointer",
    },
    KeySpec {
        key_name: "branch_hashchain",
        template: "{env}:branch:{branchId}:hashchain",
        primitive: RedisPrimitive::List,
        retention: RetentionClass::Persistent,
        notes: "Branch-level Penpal hash chain",
    },
    KeySpec {
        key_name: "lattice_coordinate",
        template: "{env}:lattice:{x}:{y}:{z}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Persistent,
        notes: "Coordinate to branch mapping",
    },
    KeySpec {
        key_name: "lattice_index",
        template: "{env}:lattice:index",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Compressed active coordinate index",
    },
    KeySpec {
        key_name: "branch_snapshot_delta",
        template: "{env}:branch:{branchId}:snapshot:{snapshotId}:delta",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Persistent,
        notes: "Snapshot delta payload",
    },
    KeySpec {
        key_name: "branch_snapshot_base",
        template: "{env}:branch:{branchId}:snapshot:{snapshotId}:base",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::Persistent,
        notes: "Full snapshot payload",
    },
    KeySpec {
        key_name: "branch_snapshots",
        template: "{env}:branch:{branchId}:snapshots",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Snapshot index by timestamp",
    },
    KeySpec {
        key_name: "branch_checkpoints",
        template: "{env}:branch:{branchId}:checkpoints",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Replay checkpoints index",
    },
    KeySpec {
        key_name: "branch_replay_state",
        template: "{env}:branch:{branchId}:replay_state",
        primitive: RedisPrimitive::Json,
        retention: RetentionClass::ShortLived,
        notes: "Optional hot replay state cache",
    },
    KeySpec {
        key_name: "queue_branch_priority",
        template: "{env}:queue:branch_priority",
        primitive: RedisPrimitive::ZSet,
        retention: RetentionClass::Persistent,
        notes: "Branch priority queue",
    },
    KeySpec {
        key_name: "queue_branch_exec",
        template: "{env}:queue:branch_exec",
        primitive: RedisPrimitive::Stream,
        retention: RetentionClass::Persistent,
        notes: "Branch execution queue",
    },
    KeySpec {
        key_name: "branches_hot",
        template: "{env}:branches:hot",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Hot branches set",
    },
    KeySpec {
        key_name: "lattice_hot",
        template: "{env}:lattice:hot",
        primitive: RedisPrimitive::Set,
        retention: RetentionClass::Persistent,
        notes: "Hot lattice slices set",
    },
    KeySpec {
        key_name: "branch_vddab_path",
        template: "{env}:branch:{branchId}:vddab_path",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Persistent,
        notes: "VDDAB disk slice mapping",
    },
    KeySpec {
        key_name: "deltashot_storage",
        template: "{env}:deltashot:{deltashotId}:storage",
        primitive: RedisPrimitive::Hash,
        retention: RetentionClass::Persistent,
        notes: "Redis/disk sync status",
    },
    KeySpec {
        key_name: "pubsub_session",
        template: "{env}:pubsub:session:{sessionId}",
        primitive: RedisPrimitive::PubSubChannel,
        retention: RetentionClass::Ephemeral,
        notes: "Session pubsub channel",
    },
    KeySpec {
        key_name: "pubsub_branch",
        template: "{env}:pubsub:branch:{branchId}",
        primitive: RedisPrimitive::PubSubChannel,
        retention: RetentionClass::Ephemeral,
        notes: "Branch pubsub channel",
    },
    KeySpec {
        key_name: "pubsub_artifact",
        template: "{env}:pubsub:artifact:{artifactId}",
        primitive: RedisPrimitive::PubSubChannel,
        retention: RetentionClass::Ephemeral,
        notes: "Artifact pubsub channel",
    },
    KeySpec {
        key_name: "lock_branch",
        template: "{env}:lock:branch:{branchId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Branch lock with TTL",
    },
    KeySpec {
        key_name: "lock_replay",
        template: "{env}:lock:replay:{branchId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Replay lock with TTL",
    },
    KeySpec {
        key_name: "lock_snapshot",
        template: "{env}:lock:snapshot:{branchId}",
        primitive: RedisPrimitive::String,
        retention: RetentionClass::Lock,
        notes: "Snapshot lock with TTL",
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisKeyspace {
    env: String,
}

impl RedisKeyspace {
    pub fn new(env: impl Into<String>) -> Result<Self, CoreError> {
        let env = env.into();
        validate_segment("env", &env)?;
        Ok(Self { env })
    }

    pub fn env(&self) -> &str {
        &self.env
    }

    pub fn specs(&self) -> &'static [KeySpec] {
        KEY_SPECS
    }

    pub fn workspace_meta(&self, workspace_id: &str) -> Result<String, CoreError> {
        self.validate_id("workspace_id", workspace_id)?;
        Ok(self.compose(&["workspace", workspace_id, "meta"]))
    }

    pub fn workspace_sessions(&self, workspace_id: &str) -> Result<String, CoreError> {
        self.validate_id("workspace_id", workspace_id)?;
        Ok(self.compose(&["workspace", workspace_id, "sessions"]))
    }

    pub fn session_meta(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "meta"]))
    }

    pub fn session_state(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "state"]))
    }

    pub fn session_messages(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "messages"]))
    }

    pub fn session_deltashots(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "deltashots"]))
    }

    pub fn session_events(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "events"]))
    }

    pub fn session_hashchain(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "hashchain"]))
    }

    pub fn session_artifacts(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "artifacts"]))
    }

    pub fn session_agent_responses(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "agent_responses"]))
    }

    pub fn session_agent_context(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "agent_context"]))
    }

    pub fn session_branches(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "branches"]))
    }

    pub fn session_active_branch(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "active_branch"]))
    }

    pub fn session_snapshot(&self, session_id: &str, snapshot_n: u64) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["session", session_id, "snapshot", &snapshot_n.to_string()]))
    }

    pub fn session_pubsub(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["pubsub", "session", session_id]))
    }

    pub fn deltashots_stream(&self) -> String {
        self.compose(&["deltashots", "stream"])
    }

    pub fn deltashot_object(&self, deltashot_id: &str) -> Result<String, CoreError> {
        self.validate_id("deltashot_id", deltashot_id)?;
        Ok(self.compose(&["deltashot", deltashot_id]))
    }

    pub fn deltashot_storage(&self, deltashot_id: &str) -> Result<String, CoreError> {
        self.validate_id("deltashot_id", deltashot_id)?;
        Ok(self.compose(&["deltashot", deltashot_id, "storage"]))
    }

    pub fn artifact_meta(&self, artifact_id: &str) -> Result<String, CoreError> {
        self.validate_id("artifact_id", artifact_id)?;
        Ok(self.compose(&["artifact", artifact_id, "meta"]))
    }

    pub fn artifact_versions(&self, artifact_id: &str) -> Result<String, CoreError> {
        self.validate_id("artifact_id", artifact_id)?;
        Ok(self.compose(&["artifact", artifact_id, "versions"]))
    }

    pub fn artifact_version_content(
        &self,
        artifact_id: &str,
        version_id: &str,
    ) -> Result<String, CoreError> {
        self.validate_id("artifact_id", artifact_id)?;
        self.validate_id("version_id", version_id)?;
        Ok(self.compose(&["artifact", artifact_id, "version", version_id]))
    }

    pub fn artifact_pubsub(&self, artifact_id: &str) -> Result<String, CoreError> {
        self.validate_id("artifact_id", artifact_id)?;
        Ok(self.compose(&["pubsub", "artifact", artifact_id]))
    }

    pub fn workflow_state(&self, workflow_id: &str) -> Result<String, CoreError> {
        self.validate_id("workflow_id", workflow_id)?;
        Ok(self.compose(&["workflow", workflow_id, "state"]))
    }

    pub fn workflow_queue(&self) -> String {
        self.compose(&["queue", "workflow"])
    }

    pub fn notion_sync_queue(&self) -> String {
        self.compose(&["queue", "notion_sync"])
    }

    pub fn agent_queue(&self) -> String {
        self.compose(&["queue", "agent"])
    }

    pub fn branch_priority_queue(&self) -> String {
        self.compose(&["queue", "branch_priority"])
    }

    pub fn branch_exec_queue(&self) -> String {
        self.compose(&["queue", "branch_exec"])
    }

    pub fn lock_session(&self, session_id: &str) -> Result<String, CoreError> {
        self.validate_id("session_id", session_id)?;
        Ok(self.compose(&["lock", "session", session_id]))
    }

    pub fn lock_artifact(&self, artifact_id: &str) -> Result<String, CoreError> {
        self.validate_id("artifact_id", artifact_id)?;
        Ok(self.compose(&["lock", "artifact", artifact_id]))
    }

    pub fn lock_workflow(&self, workflow_id: &str) -> Result<String, CoreError> {
        self.validate_id("workflow_id", workflow_id)?;
        Ok(self.compose(&["lock", "workflow", workflow_id]))
    }

    pub fn lock_branch(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["lock", "branch", branch_id]))
    }

    pub fn lock_replay(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["lock", "replay", branch_id]))
    }

    pub fn lock_snapshot(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["lock", "snapshot", branch_id]))
    }

    pub fn sessions_recent(&self) -> String {
        self.compose(&["sessions", "recent"])
    }

    pub fn sessions_active(&self) -> String {
        self.compose(&["sessions", "active"])
    }

    pub fn artifacts_hot(&self) -> String {
        self.compose(&["artifacts", "hot"])
    }

    pub fn branches_hot(&self) -> String {
        self.compose(&["branches", "hot"])
    }

    pub fn lattice_hot(&self) -> String {
        self.compose(&["lattice", "hot"])
    }

    pub fn branch_meta(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "meta"]))
    }

    pub fn branch_deltashots(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "deltashots"]))
    }

    pub fn branch_active_deltashot(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "active_ds"]))
    }

    pub fn branch_hashchain(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "hashchain"]))
    }

    pub fn branch_events(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "events"]))
    }

    pub fn branch_state(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "state"]))
    }

    pub fn branch_pubsub(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["pubsub", "branch", branch_id]))
    }

    pub fn lattice_coordinate(&self, x: i64, y: i64, z: i64) -> String {
        self.compose(&["lattice", &x.to_string(), &y.to_string(), &z.to_string()])
    }

    pub fn lattice_index(&self) -> String {
        self.compose(&["lattice", "index"])
    }

    pub fn branch_snapshot_delta(
        &self,
        branch_id: &str,
        snapshot_id: &str,
    ) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        self.validate_id("snapshot_id", snapshot_id)?;
        Ok(self.compose(&["branch", branch_id, "snapshot", snapshot_id, "delta"]))
    }

    pub fn branch_snapshot_base(
        &self,
        branch_id: &str,
        snapshot_id: &str,
    ) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        self.validate_id("snapshot_id", snapshot_id)?;
        Ok(self.compose(&["branch", branch_id, "snapshot", snapshot_id, "base"]))
    }

    pub fn branch_snapshots(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "snapshots"]))
    }

    pub fn branch_checkpoints(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "checkpoints"]))
    }

    pub fn branch_replay_state(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "replay_state"]))
    }

    pub fn branch_vddab_path(&self, branch_id: &str) -> Result<String, CoreError> {
        self.validate_id("branch_id", branch_id)?;
        Ok(self.compose(&["branch", branch_id, "vddab_path"]))
    }

    fn compose(&self, segments: &[&str]) -> String {
        let mut parts = Vec::with_capacity(segments.len() + 1);
        parts.push(self.env.clone());
        parts.extend(segments.iter().map(|segment| segment.to_string()));
        parts.join(":")
    }

    fn validate_id(&self, label: &str, id: &str) -> Result<(), CoreError> {
        validate_segment(label, id)
    }
}

fn validate_segment(label: &str, value: &str) -> Result<(), CoreError> {
    if value.trim().is_empty() {
        return Err(CoreError::InvalidInput(format!("{label} cannot be empty")));
    }
    if value.contains(':') {
        return Err(CoreError::InvalidInput(format!(
            "{label} cannot contain ':'"
        )));
    }
    Ok(())
}

pub fn key_specs() -> &'static [KeySpec] {
    KEY_SPECS
}

pub fn validate_spec_integrity() -> Result<(), CoreError> {
    let mut names = HashSet::new();
    let mut templates = HashSet::new();

    for spec in KEY_SPECS {
        if !names.insert(spec.key_name) {
            return Err(CoreError::InvalidInput(format!(
                "duplicate Redis schema key_name: {}",
                spec.key_name
            )));
        }
        if !templates.insert(spec.template) {
            return Err(CoreError::InvalidInput(format!(
                "duplicate Redis schema template: {}",
                spec.template
            )));
        }
        if spec.template.contains(":lock:") && spec.retention != RetentionClass::Lock {
            return Err(CoreError::InvalidInput(format!(
                "lock template has non-lock retention: {}",
                spec.template
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_session_keys_with_namespace() {
        let schema = RedisKeyspace::new("prod").expect("schema should initialize");
        assert_eq!(
            schema
                .session_state("abc123")
                .expect("session state key should build"),
            "prod:session:abc123:state"
        );
        assert_eq!(
            schema
                .session_events("abc123")
                .expect("session events key should build"),
            "prod:session:abc123:events"
        );
    }

    #[test]
    fn builds_branch_snapshot_and_lattice_keys() {
        let schema = RedisKeyspace::new("prod").expect("schema should initialize");
        assert_eq!(
            schema
                .branch_snapshot_delta("b1", "s42")
                .expect("delta key should build"),
            "prod:branch:b1:snapshot:s42:delta"
        );
        assert_eq!(schema.lattice_coordinate(1, 2, 3), "prod:lattice:1:2:3");
    }

    #[test]
    fn rejects_invalid_key_segments() {
        assert!(RedisKeyspace::new("").is_err());

        let schema = RedisKeyspace::new("prod").expect("schema should initialize");
        assert!(schema.session_state("bad:id").is_err());
        assert!(schema.branch_meta("").is_err());
    }

    #[test]
    fn spec_integrity_is_enforced() {
        validate_spec_integrity().expect("spec integrity should pass");
        assert!(key_specs()
            .iter()
            .any(|spec| spec.key_name == "queue_branch_priority"));
        assert!(key_specs()
            .iter()
            .any(|spec| spec.key_name == "notion_sync_queue"));
        assert!(key_specs()
            .iter()
            .any(|spec| spec.key_name == "session_active_branch"));
        assert!(key_specs()
            .iter()
            .any(|spec| spec.key_name == "branch_state"));
        assert!(key_specs()
            .iter()
            .any(|spec| spec.key_name == "lock_session"));
    }
}
