use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use delta_core::redis_schema::RedisKeyspace;
use deltashot::{DeltaShot, Metadata, Op};
use redis::aio::ConnectionManager;
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
            .read_hash_field(&object_key, "branch_id")
            .await?
            .ok_or_else(|| anyhow!("missing branch_id for deltashot {}", deltashot_id))?;
        Ok(self
            .deltashot_store
            .absolute_ops_path(&branch_id, deltashot_id))
    }

    pub async fn store_deltashot(&self, delta: &DeltaShot, compressed_ops: &[u8]) -> Result<()> {
        let ops_path = self
            .deltashot_store
            .write_compressed_ops(&delta.branch_id, &delta.id, compressed_ops)
            .await
            .with_context(|| format!("failed to write ops for deltashot '{}'", delta.id))?;
        let rel_path = DeltaShotStore::relative_ops_path(&delta.branch_id, &delta.id);
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
            .branch_deltashots(&delta.branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let session_key = self
            .keyspace
            .session_deltashots(&delta.session_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        let active_ds_key = self
            .keyspace
            .branch_active_deltashot(&delta.branch_id)
            .map_err(|err| anyhow!(err.to_string()))?;

        let mut conn = self.redis.clone();
        let _: () = redis::cmd("HSET")
            .arg(&delta_key)
            .arg("session_id")
            .arg(&delta.session_id)
            .arg("branch_id")
            .arg(&delta.branch_id)
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
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to write deltashot object '{}'", delta.id))?;
        if let Some(agent) = delta.metadata.agent.as_ref() {
            let _: () = redis::cmd("HSET")
                .arg(&delta_key)
                .arg("agent")
                .arg(agent)
                .query_async(&mut conn)
                .await
                .with_context(|| format!("failed to write agent metadata for '{}'", delta.id))?;
        }
        if let Some(workflow_step) = delta.metadata.workflow_step.as_ref() {
            let _: () = redis::cmd("HSET")
                .arg(&delta_key)
                .arg("workflow_step")
                .arg(workflow_step)
                .query_async(&mut conn)
                .await
                .with_context(|| format!("failed to write workflow metadata for '{}'", delta.id))?;
        }
        let _: () = redis::cmd("HSET")
            .arg(&storage_key)
            .arg("backend")
            .arg("vddab")
            .arg("vddab_rel_path")
            .arg(&rel_path)
            .arg("storage_path")
            .arg(ops_path.to_string_lossy().to_string())
            .arg("compressed_ops")
            .arg(compressed_ops)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to write storage mapping for '{}'", delta.id))?;
        let _: () = redis::cmd("ZADD")
            .arg(&branch_key)
            .arg(delta.timestamp.to_string())
            .arg(&delta.id)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to append branch chain for '{}'", delta.branch_id))?;
        let _: () = redis::cmd("ZADD")
            .arg(&session_key)
            .arg(delta.timestamp.to_string())
            .arg(&delta.id)
            .query_async(&mut conn)
            .await
            .with_context(|| {
                format!("failed to append session chain for '{}'", delta.session_id)
            })?;
        let _: () = redis::cmd("SET")
            .arg(&active_ds_key)
            .arg(&delta.id)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("failed to set active ds for '{}'", delta.branch_id))?;
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
