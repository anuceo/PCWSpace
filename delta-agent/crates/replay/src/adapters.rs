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
        tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read compressed ops from {}", path.display()))
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
