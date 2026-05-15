use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use delta_core::redis_schema::RedisKeyspace;
use deltashot::{DeltaShot, Metadata, Op};
use redis::aio::ConnectionManager;
use redis::Script;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vddab::{deltashot_store::DeltaShotStore, snapshot_store::SnapshotStore};

use crate::verifier::{decompress_ops, DeltaRepository, StateRepository};

#[derive(Clone)]
pub struct RedisVddabRepository {
    redis: ConnectionManager,
    keyspace: RedisKeyspace,
    deltashot_store: DeltaShotStore,
    snapshot_store: SnapshotStore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableWorkflowJob {
    pub workflow_id: String,
    pub session_id: String,
    pub step: String,
    pub payload: String,
    pub timestamp_ms: u64,
    pub attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableNotionSyncJob {
    pub session_id: String,
    pub summary: String,
    pub artifacts: Vec<String>,
    pub timestamp_ms: u64,
    pub attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWorkspaceRecord {
    pub workspace_id: String,
    pub name: String,
    pub created_at: u64,
    pub owner_id: String,
    pub active_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSessionMeta {
    pub session_id: String,
    pub workspace_id: String,
    pub name: String,
    pub created_at: u64,
    pub status: String,
    pub workflow_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWorkflowRecord {
    pub workflow_id: String,
    pub session_id: String,
    pub status: String,
    pub current_step: String,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedArtifactVersion {
    pub version: u64,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedArtifactRecord {
    pub artifact_id: String,
    pub session_id: String,
    pub branch_id: String,
    pub artifact_type: String,
    pub created_at: u64,
    pub current_version: u64,
    pub versions: Vec<PersistedArtifactVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSessionMessage {
    pub id: String,
    #[serde(default)]
    pub branch_id: String,
    pub role: String,
    pub content: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSessionEvent {
    pub id: String,
    #[serde(default)]
    pub branch_id: String,
    pub event_type: String,
    pub payload: Value,
    pub timestamp: u64,
}

impl RedisVddabRepository {
    pub async fn connect(
        redis_url: &str,
        keyspace: RedisKeyspace,
        vddab_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .with_context(|| format!("invalid redis url '{}'", redis_url))?;
        let redis = client
            .get_connection_manager()
            .await
            .context("failed to create redis connection manager")?;
        let root = vddab_root.into();

        Ok(Self {
            redis,
            keyspace,
            deltashot_store: DeltaShotStore::new(root.clone()),
            snapshot_store: SnapshotStore::new(root),
        })
    }

    async fn load_deltashot(&self, deltashot_id: &str) -> Result<DeltaShot> {
        let key = self
            .keyspace
            .deltashot_object(deltashot_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();

        let payload: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("payload")
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read payload for {}", deltashot_id))?;
        if let Some(payload) = payload {
            let mut delta = serde_json::from_str::<DeltaShot>(&payload)
                .with_context(|| format!("invalid deltashot payload for {}", deltashot_id))?;
            if delta.ops.is_empty() {
                delta.ops = self.load_ops(deltashot_id).await.unwrap_or_default();
            }
            return Ok(delta);
        }

        let session_id = self
            .read_hash_field(&key, "session_id")
            .await?
            .unwrap_or_default();
        let branch_id = self
            .read_hash_field(&key, "branch_id")
            .await?
            .unwrap_or_default();
        let prev_hash = self
            .read_hash_field(&key, "prev_hash")
            .await?
            .unwrap_or_default();
        let hash = self
            .read_hash_field(&key, "hash")
            .await?
            .unwrap_or_default();
        let timestamp = self
            .read_hash_field(&key, "timestamp")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u128>()
            .unwrap_or(0);
        let event_type = self
            .read_hash_field(&key, "event_type")
            .await?
            .unwrap_or_else(|| "STATE_UPDATE".to_owned());
        let agent = self.read_hash_field(&key, "agent").await?;
        let workflow_step = self.read_hash_field(&key, "workflow_step").await?;
        let artifacts = self
            .read_hash_field(&key, "artifacts")
            .await?
            .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
            .unwrap_or_default();
        let ops = self.load_ops(deltashot_id).await.unwrap_or_default();

        Ok(DeltaShot {
            id: deltashot_id.to_owned(),
            session_id,
            branch_id,
            prev_hash,
            hash,
            timestamp,
            ops,
            artifacts,
            metadata: Metadata {
                event_type,
                agent,
                workflow_step,
            },
        })
    }

    async fn read_hash_field(&self, key: &str, field: &str) -> Result<Option<String>> {
        let mut conn = self.redis.clone();
        let value: Option<String> = redis::cmd("HGET")
            .arg(key)
            .arg(field)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read redis hash field '{}:{}'", key, field))?;
        Ok(value)
    }

    async fn read_vddab_path(&self, deltashot_id: &str) -> Result<PathBuf> {
        let storage_key = self
            .keyspace
            .deltashot_storage(deltashot_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        if let Some(rel_path) = self.read_hash_field(&storage_key, "vddab_rel_path").await? {
            return Ok(self.deltashot_store.root.join(rel_path));
        }

        let object_key = self
            .keyspace
            .deltashot_object(deltashot_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let branch_id = self
            .read_hash_field(&object_key, "storage_branch_id")
            .await?
            .or(self.read_hash_field(&object_key, "branch_id").await?)
            .ok_or_else(|| anyhow!("missing branch_id for deltashot {}", deltashot_id))?;
        Ok(self
            .deltashot_store
            .absolute_ops_path(&branch_id, deltashot_id))
    }

    pub async fn store_deltashot(
        &self,
        delta: &DeltaShot,
        storage_branch_id: &str,
        compressed_ops: &[u8],
    ) -> Result<()> {
        let ops_path = self
            .deltashot_store
            .write_compressed_ops(storage_branch_id, &delta.id, compressed_ops)
            .await
            .with_context(|| format!("failed to write ops for deltashot '{}'", delta.id))?;
        let rel_path = DeltaShotStore::relative_ops_path(storage_branch_id, &delta.id);
        let rel_path = rel_path.to_string_lossy().to_string();
        let payload =
            serde_json::to_string(delta).with_context(|| format!("serialize '{}'", delta.id))?;
        let ops_json = serde_json::to_string(&delta.ops)
            .with_context(|| format!("serialize '{}'", delta.id))?;
        let artifacts_json = serde_json::to_string(&delta.artifacts)
            .with_context(|| format!("serialize artifacts for '{}'", delta.id))?;

        let delta_key = self
            .keyspace
            .deltashot_object(&delta.id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let storage_key = self
            .keyspace
            .deltashot_storage(&delta.id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let branch_key = self
            .keyspace
            .branch_deltashots(storage_branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let session_key = self
            .keyspace
            .session_deltashots(&delta.session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let active_ds_key = self
            .keyspace
            .branch_active_deltashot(storage_branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;

        let mut conn = self.redis.clone();
        let mut tx = redis::pipe();
        tx.atomic()
            .cmd("HSET")
            .arg(&delta_key)
            .arg("session_id")
            .arg(&delta.session_id)
            .arg("branch_id")
            .arg(&delta.branch_id)
            .arg("storage_branch_id")
            .arg(storage_branch_id)
            .arg("prev_hash")
            .arg(&delta.prev_hash)
            .arg("hash")
            .arg(&delta.hash)
            .arg("timestamp")
            .arg(delta.timestamp.to_string())
            .arg("event_type")
            .arg(&delta.metadata.event_type)
            .arg("ops")
            .arg(&ops_json)
            .arg("artifacts")
            .arg(&artifacts_json)
            .arg("payload")
            .arg(&payload)
            .ignore()
            .cmd("HSET")
            .arg(&storage_key)
            .arg("backend")
            .arg("vddab")
            .arg("vddab_rel_path")
            .arg(&rel_path)
            .arg("storage_path")
            .arg(ops_path.to_string_lossy().to_string())
            .arg("compressed_ops")
            .arg(compressed_ops)
            .ignore()
            .cmd("ZADD")
            .arg(&branch_key)
            .arg(delta.timestamp.to_string())
            .arg(&delta.id)
            .ignore()
            .cmd("ZADD")
            .arg(&session_key)
            .arg(delta.timestamp.to_string())
            .arg(&delta.id)
            .ignore()
            .cmd("SET")
            .arg(&active_ds_key)
            .arg(&delta.id)
            .ignore();
        if let Some(agent) = delta.metadata.agent.as_ref() {
            tx.cmd("HSET")
                .arg(&delta_key)
                .arg("agent")
                .arg(agent)
                .ignore();
        } else {
            tx.cmd("HDEL").arg(&delta_key).arg("agent").ignore();
        }
        if let Some(workflow_step) = delta.metadata.workflow_step.as_ref() {
            tx.cmd("HSET")
                .arg(&delta_key)
                .arg("workflow_step")
                .arg(workflow_step)
                .ignore();
        } else {
            tx.cmd("HDEL").arg(&delta_key).arg("workflow_step").ignore();
        }
        let _: () = tx
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to transactionally store deltashot '{}'", delta.id))?;
        Ok(())
    }

    pub async fn replace_branch_chain(
        &self,
        branch_id: &str,
        deltashot_ids: &[String],
    ) -> Result<()> {
        let branch_key = self
            .keyspace
            .branch_deltashots(branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("DEL")
            .arg(&branch_key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to clear branch chain '{}'", branch_id))?;
        if !deltashot_ids.is_empty() {
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(&branch_key);
            for (index, deltashot_id) in deltashot_ids.iter().enumerate() {
                cmd.arg(index.to_string()).arg(deltashot_id);
            }
            let _: () = cmd
                .query_async(&mut conn)
                .await
                .with_context(|| format!("failed to write branch chain '{}'", branch_id))?;
            let active_ds_key = self
                .keyspace
                .branch_active_deltashot(branch_id)
                .map_err(|err| anyhow!(err.to_string()))?;
            if let Some(last) = deltashot_ids.last() {
                let _: () = redis::cmd("SET")
                    .arg(active_ds_key)
                    .arg(last)
                    .query_async(&mut conn)
                    .await
                    .with_context(|| format!("failed to write active ds for '{}'", branch_id))?;
            }
        }
        Ok(())
    }

    pub async fn store_branch_state(&self, branch_id: &str, state: &Value) -> Result<()> {
        let key = self
            .keyspace
            .branch_state(branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let payload = serde_json::to_string(state)
            .with_context(|| format!("serialize state '{}'", branch_id))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to write branch state '{}'", branch_id))?;
        Ok(())
    }

    pub async fn store_snapshot(
        &self,
        branch_id: &str,
        start_index: usize,
        state: &Value,
    ) -> Result<()> {
        let _ = self
            .snapshot_store
            .write_snapshot(branch_id, start_index, state)
            .await
            .with_context(|| format!("failed to write snapshot for '{}'", branch_id))?;
        let index_key = self
            .keyspace
            .branch_snapshots(branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("ZADD")
            .arg(index_key)
            .arg(start_index.to_string())
            .arg(start_index.to_string())
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to index snapshot for '{}'", branch_id))?;
        Ok(())
    }

    pub async fn set_session_active_branch(&self, session_id: &str, branch_id: &str) -> Result<()> {
        let active_key = self
            .keyspace
            .session_active_branch(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let branches_key = self
            .keyspace
            .session_branches(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("SET")
            .arg(active_key)
            .arg(branch_id)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "failed to set active branch '{}' for session '{}'",
                    branch_id, session_id
                )
            })?;
        let _: () = redis::cmd("SADD")
            .arg(branches_key)
            .arg(branch_id)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "failed to register branch '{}' for session '{}'",
                    branch_id, session_id
                )
            })?;
        Ok(())
    }

    pub async fn register_session_branch(&self, session_id: &str, branch_id: &str) -> Result<()> {
        let branches_key = self
            .keyspace
            .session_branches(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("SADD")
            .arg(branches_key)
            .arg(branch_id)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "failed to register branch '{}' for session '{}'",
                    branch_id, session_id
                )
            })?;
        Ok(())
    }

    pub async fn list_sessions(&self) -> Result<Vec<String>> {
        let mut cursor = 0u64;
        let pattern = format!("{}:session:*:branches", self.keyspace.env());
        let mut sessions = HashSet::new();
        let mut conn = self.redis.clone();
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut conn)
                .await
                .context("failed to scan session branch keys")?;
            for key in keys {
                let parts = key.split(':').collect::<Vec<_>>();
                if parts.len() >= 4 && parts[1] == "session" {
                    sessions.insert(parts[2].to_owned());
                }
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        let mut ordered = sessions.into_iter().collect::<Vec<_>>();
        ordered.sort();
        Ok(ordered)
    }

    pub async fn list_session_branches(&self, session_id: &str) -> Result<Vec<String>> {
        let branches_key = self
            .keyspace
            .session_branches(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let mut branches: Vec<String> = redis::cmd("SMEMBERS")
            .arg(branches_key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to load branches for session '{}'", session_id))?;
        branches.sort();
        Ok(branches)
    }

    pub async fn get_session_active_branch(&self, session_id: &str) -> Result<Option<String>> {
        let active_key = self
            .keyspace
            .session_active_branch(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let active: Option<String> = redis::cmd("GET")
            .arg(active_key)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "failed to load active branch pointer for session '{}'",
                    session_id
                )
            })?;
        Ok(active)
    }

    pub async fn store_workspace(&self, workspace: &PersistedWorkspaceRecord) -> Result<()> {
        let key = self
            .keyspace
            .workspace_meta(&workspace.workspace_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("HSET")
            .arg(&key)
            .arg("name")
            .arg(&workspace.name)
            .arg("created_at")
            .arg(workspace.created_at.to_string())
            .arg("owner_id")
            .arg(&workspace.owner_id)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to persist workspace '{}'", workspace.workspace_id))?;
        if let Some(active) = workspace.active_session_id.as_ref() {
            let _: () = redis::cmd("HSET")
                .arg(&key)
                .arg("active_session_id")
                .arg(active)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to persist workspace active session '{}'",
                        workspace.workspace_id
                    )
                })?;
        } else {
            let _: () = redis::cmd("HDEL")
                .arg(&key)
                .arg("active_session_id")
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to clear workspace active session '{}'",
                        workspace.workspace_id
                    )
                })?;
        }
        Ok(())
    }

    pub async fn list_workspaces(&self) -> Result<Vec<PersistedWorkspaceRecord>> {
        let mut cursor = 0u64;
        let pattern = format!("{}:workspace:*:meta", self.keyspace.env());
        let mut workspace_ids = HashSet::new();
        let mut conn = self.redis.clone();
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut conn)
                .await
                .context("failed to scan workspace keys")?;
            for key in keys {
                let parts = key.split(':').collect::<Vec<_>>();
                if parts.len() >= 4 && parts[1] == "workspace" {
                    workspace_ids.insert(parts[2].to_owned());
                }
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        let mut ids = workspace_ids.into_iter().collect::<Vec<_>>();
        ids.sort();

        let mut result = Vec::new();
        for workspace_id in ids {
            if let Some(workspace) = self.get_workspace(&workspace_id).await? {
                result.push(workspace);
            }
        }
        Ok(result)
    }

    pub async fn get_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<PersistedWorkspaceRecord>> {
        let key = self
            .keyspace
            .workspace_meta(workspace_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let name = self.read_hash_field(&key, "name").await?;
        let Some(name) = name else {
            return Ok(None);
        };
        let created_at = self
            .read_hash_field(&key, "created_at")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        let owner_id = self
            .read_hash_field(&key, "owner_id")
            .await?
            .unwrap_or_else(|| "system".to_owned());
        let active_session_id = self.read_hash_field(&key, "active_session_id").await?;
        Ok(Some(PersistedWorkspaceRecord {
            workspace_id: workspace_id.to_owned(),
            name,
            created_at,
            owner_id,
            active_session_id,
        }))
    }

    pub async fn link_workspace_session(
        &self,
        workspace_id: &str,
        session_id: &str,
        created_at: u64,
    ) -> Result<()> {
        let key = self
            .keyspace
            .workspace_sessions(workspace_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("ZADD")
            .arg(key)
            .arg(created_at.to_string())
            .arg(session_id)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!(
                    "failed to link session '{}' to workspace '{}'",
                    session_id, workspace_id
                )
            })?;
        Ok(())
    }

    pub async fn get_workspace_sessions(&self, workspace_id: &str) -> Result<Vec<(u64, String)>> {
        let key = self
            .keyspace
            .workspace_sessions(workspace_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let values: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(key)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to load workspace sessions '{}'", workspace_id))?;
        Ok(values
            .into_iter()
            .map(|(session_id, score)| (score as u64, session_id))
            .collect::<Vec<_>>())
    }

    pub async fn store_session_meta(&self, session: &PersistedSessionMeta) -> Result<()> {
        let key = self
            .keyspace
            .session_meta(&session.session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("HSET")
            .arg(&key)
            .arg("workspace_id")
            .arg(&session.workspace_id)
            .arg("name")
            .arg(&session.name)
            .arg("created_at")
            .arg(session.created_at.to_string())
            .arg("status")
            .arg(&session.status)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to persist session meta '{}'", session.session_id))?;
        if let Some(workflow_id) = session.workflow_id.as_ref() {
            let _: () = redis::cmd("HSET")
                .arg(&key)
                .arg("workflow_id")
                .arg(workflow_id)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to persist session workflow id '{}'",
                        session.session_id
                    )
                })?;
        } else {
            let _: () = redis::cmd("HDEL")
                .arg(&key)
                .arg("workflow_id")
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!("failed to clear workflow id for '{}'", session.session_id)
                })?;
        }
        Ok(())
    }

    pub async fn get_session_meta(&self, session_id: &str) -> Result<Option<PersistedSessionMeta>> {
        let key = self
            .keyspace
            .session_meta(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let workspace_id = self.read_hash_field(&key, "workspace_id").await?;
        let Some(workspace_id) = workspace_id else {
            return Ok(None);
        };
        let name = self
            .read_hash_field(&key, "name")
            .await?
            .unwrap_or_else(|| "Rehydrated Session".to_owned());
        let created_at = self
            .read_hash_field(&key, "created_at")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        let status = self
            .read_hash_field(&key, "status")
            .await?
            .unwrap_or_else(|| "active".to_owned());
        let workflow_id = self.read_hash_field(&key, "workflow_id").await?;
        Ok(Some(PersistedSessionMeta {
            session_id: session_id.to_owned(),
            workspace_id,
            name,
            created_at,
            status,
            workflow_id,
        }))
    }

    pub async fn store_workflow_state(&self, workflow: &PersistedWorkflowRecord) -> Result<()> {
        let key = self
            .keyspace
            .workflow_state(&workflow.workflow_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("HSET")
            .arg(&key)
            .arg("session_id")
            .arg(&workflow.session_id)
            .arg("status")
            .arg(&workflow.status)
            .arg("current_step")
            .arg(&workflow.current_step)
            .arg("updated_at")
            .arg(workflow.updated_at.to_string())
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to persist workflow '{}'", workflow.workflow_id))?;
        Ok(())
    }

    pub async fn get_workflow_state(
        &self,
        workflow_id: &str,
    ) -> Result<Option<PersistedWorkflowRecord>> {
        let key = self
            .keyspace
            .workflow_state(workflow_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let session_id = self.read_hash_field(&key, "session_id").await?;
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let status = self
            .read_hash_field(&key, "status")
            .await?
            .unwrap_or_else(|| "active".to_owned());
        let current_step = self
            .read_hash_field(&key, "current_step")
            .await?
            .unwrap_or_else(|| "start".to_owned());
        let updated_at = self
            .read_hash_field(&key, "updated_at")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        Ok(Some(PersistedWorkflowRecord {
            workflow_id: workflow_id.to_owned(),
            session_id,
            status,
            current_step,
            updated_at,
        }))
    }

    pub async fn list_workflow_ids(&self) -> Result<Vec<String>> {
        let mut cursor = 0u64;
        let pattern = format!("{}:workflow:*:state", self.keyspace.env());
        let mut workflow_ids = HashSet::new();
        let mut conn = self.redis.clone();
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut conn)
                .await
                .context("failed to scan workflow keys")?;
            for key in keys {
                let parts = key.split(':').collect::<Vec<_>>();
                if parts.len() >= 4 && parts[1] == "workflow" {
                    workflow_ids.insert(parts[2].to_owned());
                }
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        let mut ids = workflow_ids.into_iter().collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub async fn store_artifact(&self, artifact: &PersistedArtifactRecord) -> Result<()> {
        let meta_key = self
            .keyspace
            .artifact_meta(&artifact.artifact_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let versions_key = self
            .keyspace
            .artifact_versions(&artifact.artifact_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let session_artifacts_key = self
            .keyspace
            .session_artifacts(&artifact.session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let _: () = redis::cmd("HSET")
            .arg(&meta_key)
            .arg("session_id")
            .arg(&artifact.session_id)
            .arg("branch_id")
            .arg(&artifact.branch_id)
            .arg("artifact_type")
            .arg(&artifact.artifact_type)
            .arg("created_at")
            .arg(artifact.created_at.to_string())
            .arg("current_version")
            .arg(artifact.current_version.to_string())
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!("failed to persist artifact meta '{}'", artifact.artifact_id)
            })?;
        let _: () = redis::cmd("SADD")
            .arg(session_artifacts_key)
            .arg(&artifact.artifact_id)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to link artifact '{}'", artifact.artifact_id))?;
        for version in &artifact.versions {
            let version_id = version.version.to_string();
            let content_key = self
                .keyspace
                .artifact_version_content(&artifact.artifact_id, &version_id)
                .map_err(|err| anyhow!(err.to_string()))?;
            let _: () = redis::cmd("SET")
                .arg(content_key)
                .arg(&version.content)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to persist artifact content '{}' version {}",
                        artifact.artifact_id, version.version
                    )
                })?;
            let _: () = redis::cmd("ZADD")
                .arg(&versions_key)
                .arg(version.version.to_string())
                .arg(&version_id)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to persist artifact version '{}' version {}",
                        artifact.artifact_id, version.version
                    )
                })?;
        }
        Ok(())
    }

    pub async fn get_artifact(&self, artifact_id: &str) -> Result<Option<PersistedArtifactRecord>> {
        let meta_key = self
            .keyspace
            .artifact_meta(artifact_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let session_id = self.read_hash_field(&meta_key, "session_id").await?;
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let branch_id = self
            .read_hash_field(&meta_key, "branch_id")
            .await?
            .unwrap_or_else(|| "br_main".to_owned());
        let artifact_type = self
            .read_hash_field(&meta_key, "artifact_type")
            .await?
            .unwrap_or_else(|| "code".to_owned());
        let created_at = self
            .read_hash_field(&meta_key, "created_at")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        let current_version = self
            .read_hash_field(&meta_key, "current_version")
            .await?
            .unwrap_or_else(|| "0".to_owned())
            .parse::<u64>()
            .unwrap_or(0);
        let versions_key = self
            .keyspace
            .artifact_versions(artifact_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let version_ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(versions_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to load artifact versions '{}'", artifact_id))?;
        let mut versions = Vec::with_capacity(version_ids.len());
        for version_id in version_ids {
            let version = version_id.parse::<u64>().unwrap_or(0);
            let content_key = self
                .keyspace
                .artifact_version_content(artifact_id, &version_id)
                .map_err(|err| anyhow!(err.to_string()))?;
            let content: Option<String> = redis::cmd("GET")
                .arg(content_key)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "failed to load artifact content '{}' version {}",
                        artifact_id, version
                    )
                })?;
            if let Some(content) = content {
                versions.push(PersistedArtifactVersion { version, content });
            }
        }
        Ok(Some(PersistedArtifactRecord {
            artifact_id: artifact_id.to_owned(),
            session_id,
            branch_id,
            artifact_type,
            created_at,
            current_version,
            versions,
        }))
    }

    pub async fn list_session_artifacts(&self, session_id: &str) -> Result<Vec<String>> {
        let key = self
            .keyspace
            .session_artifacts(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let mut artifact_ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to load session artifacts '{}'", session_id))?;
        artifact_ids.sort();
        Ok(artifact_ids)
    }

    pub async fn try_acquire_distributed_lock(
        &self,
        lock_key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> Result<bool> {
        let mut conn = self.redis.clone();
        let reply: Option<String> = redis::cmd("SET")
            .arg(lock_key)
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to acquire distributed lock '{}'", lock_key))?;
        Ok(reply.is_some())
    }

    pub async fn release_distributed_lock(&self, lock_key: &str, owner: &str) -> Result<()> {
        let mut conn = self.redis.clone();
        let script = Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
        );
        let _: i64 = script
            .key(lock_key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .with_context(|| format!("failed to release distributed lock '{}'", lock_key))?;
        Ok(())
    }

    pub async fn store_idempotency_payload(
        &self,
        key: &str,
        payload: &str,
        ttl_secs: u64,
    ) -> Result<()> {
        let mut conn = self.redis.clone();
        let _: String = redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to store idempotency payload '{}'", key))?;
        Ok(())
    }

    pub async fn load_idempotency_payload(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.redis.clone();
        let payload: Option<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to load idempotency payload '{}'", key))?;
        Ok(payload)
    }

    pub async fn append_session_message(
        &self,
        session_id: &str,
        message: &PersistedSessionMessage,
    ) -> Result<()> {
        let key = self
            .keyspace
            .session_messages(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.enqueue_stream_payload(&key, message).await
    }

    pub async fn append_session_event(
        &self,
        session_id: &str,
        event: &PersistedSessionEvent,
    ) -> Result<()> {
        let key = self
            .keyspace
            .session_events(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.enqueue_stream_payload(&key, event).await
    }

    pub async fn load_session_messages(
        &self,
        session_id: &str,
    ) -> Result<Vec<PersistedSessionMessage>> {
        let key = self
            .keyspace
            .session_messages(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.load_stream_payloads(&key).await
    }

    pub async fn load_session_events(
        &self,
        session_id: &str,
    ) -> Result<Vec<PersistedSessionEvent>> {
        let key = self
            .keyspace
            .session_events(session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.load_stream_payloads(&key).await
    }

    pub async fn enqueue_workflow_job(&self, job: &DurableWorkflowJob) -> Result<()> {
        let key = self.keyspace.workflow_queue();
        self.enqueue_stream_payload(&key, job).await
    }

    pub async fn dequeue_workflow_job(&self) -> Result<Option<DurableWorkflowJob>> {
        let key = self.keyspace.workflow_queue();
        self.dequeue_stream_payload(&key).await
    }

    pub async fn enqueue_notion_sync_job(&self, job: &DurableNotionSyncJob) -> Result<()> {
        let key = self.keyspace.notion_sync_queue();
        self.enqueue_stream_payload(&key, job).await
    }

    pub async fn dequeue_notion_sync_job(&self) -> Result<Option<DurableNotionSyncJob>> {
        let key = self.keyspace.notion_sync_queue();
        self.dequeue_stream_payload(&key).await
    }

    async fn enqueue_stream_payload<T: Serialize>(&self, key: &str, payload: &T) -> Result<()> {
        let data = serde_json::to_string(payload).context("failed to serialize queue payload")?;
        let mut conn = self.redis.clone();
        let _: String = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("payload")
            .arg(data)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to append stream payload for key '{}'", key))?;
        Ok(())
    }

    async fn load_stream_payloads<T: DeserializeOwned>(&self, key: &str) -> Result<Vec<T>> {
        let mut conn = self.redis.clone();
        let range: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read stream payloads for key '{}'", key))?;
        let mut decoded = Vec::with_capacity(range.ids.len());
        for entry in range.ids {
            if let Some(payload) = entry
                .map
                .get("payload")
                .and_then(|value| redis::from_redis_value::<String>(value).ok())
            {
                let item = serde_json::from_str::<T>(&payload)
                    .with_context(|| format!("invalid stream payload in key '{}'", key))?;
                decoded.push(item);
            }
        }
        Ok(decoded)
    }

    async fn dequeue_stream_payload<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.redis.clone();
        let range: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read stream payload for key '{}'", key))?;

        let Some(entry) = range.ids.into_iter().next() else {
            return Ok(None);
        };
        let payload = entry
            .map
            .get("payload")
            .and_then(|value| redis::from_redis_value::<String>(value).ok())
            .ok_or_else(|| anyhow!("stream entry {} missing payload field", entry.id))?;
        let decoded = serde_json::from_str::<T>(&payload)
            .with_context(|| format!("invalid stream payload in key '{}'", key))?;
        let _: i64 = redis::cmd("XDEL")
            .arg(key)
            .arg(&entry.id)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to ack stream payload for key '{}'", key))?;
        Ok(Some(decoded))
    }
}

impl DeltaRepository for RedisVddabRepository {
    async fn load_branch_chain(&self, branch_id: &str) -> Result<Vec<DeltaShot>> {
        let key = self
            .keyspace
            .branch_deltashots(branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read branch chain for '{}'", branch_id))?;

        let mut deltas = Vec::with_capacity(ids.len());
        for id in ids {
            deltas.push(self.load_deltashot(&id).await?);
        }
        Ok(deltas)
    }

    async fn load_ops(&self, deltashot_id: &str) -> Result<Vec<Op>> {
        let key = self
            .keyspace
            .deltashot_object(deltashot_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        if let Some(raw_ops) = self.read_hash_field(&key, "ops").await? {
            let ops = serde_json::from_str::<Vec<Op>>(&raw_ops)
                .with_context(|| format!("invalid ops payload for {}", deltashot_id))?;
            return Ok(ops);
        }

        let compressed = self.load_compressed_ops(deltashot_id).await?;
        let decompressed = decompress_ops(&compressed)?;
        let ops = serde_json::from_slice::<Vec<Op>>(&decompressed)
            .with_context(|| format!("invalid compressed ops payload for {}", deltashot_id))?;
        Ok(ops)
    }

    async fn load_compressed_ops(&self, deltashot_id: &str) -> Result<Vec<u8>> {
        let path = self.read_vddab_path(deltashot_id).await?;
        if let Ok(payload) = tokio::fs::read(&path).await {
            return Ok(payload);
        }

        let key = self
            .keyspace
            .deltashot_storage(deltashot_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let payload: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&key)
            .arg("compressed_ops")
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read compressed ops from redis for {}", key))?;
        payload.ok_or_else(|| anyhow!("compressed ops not found for {}", deltashot_id))
    }
}

impl StateRepository for RedisVddabRepository {
    async fn get_branch_state(&self, branch_id: &str) -> Result<Value> {
        let key = self
            .keyspace
            .branch_state(branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let mut conn = self.redis.clone();
        let payload: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to read branch state for '{}'", branch_id))?;

        if let Some(payload) = payload {
            return serde_json::from_str(&payload)
                .with_context(|| format!("invalid branch state payload for '{}'", branch_id));
        }

        Ok(Value::Object(serde_json::Map::new()))
    }

    async fn load_nearest_snapshot(&self, branch_id: &str) -> Result<(Value, usize)> {
        let snapshot = self
            .snapshot_store
            .read_nearest_snapshot(branch_id)
            .await
            .with_context(|| format!("failed to read vddab snapshot for '{}'", branch_id))?;
        Ok(snapshot.unwrap_or((Value::Object(serde_json::Map::new()), 0)))
    }
}
