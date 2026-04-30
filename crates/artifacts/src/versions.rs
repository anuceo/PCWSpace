use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{Artifact, AgentType, new_id, now},
};
use infra::redis_client::{key_artifact, key_artifact_versions, key_artifact_latest};
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use tracing::instrument;

/// Intermediate version snapshots expire this long after being superseded.
/// The root artifact (version 1) is exempt — it is the permanent metadata anchor.
const STALE_SNAPSHOT_TTL_SECS: i64 = 7 * 24 * 3600; // 7 days

/// Create a new version of an existing artifact.
/// `parent_id` must be the **root** artifact ID (version 1). Every version in
/// the chain is anchored there: the version list, the latest pointer, and the
/// stale-snapshot TTL logic all key off the root.
#[instrument(skip(conn))]
pub async fn new_version(
    parent_id: &str,
    new_content: &str,
    agent_type: Option<AgentType>,
    deltashot_id: Option<&str>,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Artifact> {
    // Load root for stable metadata (name, type, linked_session)
    let root: Artifact = {
        let raw: Option<String> = conn
            .get(key_artifact(parent_id))
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        raw.ok_or_else(|| PcwError::ArtifactNotFound(parent_id.to_string()))
            .and_then(|s| serde_json::from_str(&s).map_err(|e| PcwError::SerializationError(e.to_string())))?
    };

    // Derive next version number from the current latest, not the root.
    // Root is always v1 so `root.version + 1` would give v2 forever.
    let current_latest_id: Option<String> = conn
        .get(key_artifact_latest(parent_id))
        .await
        .unwrap_or(None);

    let next_version = if let Some(ref lid) = current_latest_id {
        let raw: Option<String> = conn
            .get(key_artifact(lid))
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        raw.and_then(|s| serde_json::from_str::<Artifact>(&s).ok())
            .map(|a| a.version + 1)
            .unwrap_or(root.version + 1)
    } else {
        root.version + 1
    };

    let content_hash = hex::encode(Sha256::digest(new_content.as_bytes()));

    let new_artifact = Artifact {
        artifact_id: new_id(),
        name: root.name.clone(),
        artifact_type: root.artifact_type.clone(),
        content: new_content.to_string(),
        content_hash,
        version: next_version,
        parent_version_id: Some(parent_id.to_string()),
        linked_session: root.linked_session.clone(),
        agent_type,
        deltashot_id: deltashot_id.map(str::to_string),
        created_at: now(),
        metadata: Default::default(),
    };

    let json = serde_json::to_string(&new_artifact)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let _: () = conn
        .set(key_artifact(&new_artifact.artifact_id), json)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    // Append to root's version list
    let _: () = conn
        .rpush(key_artifact_versions(parent_id), &new_artifact.artifact_id)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    // Schedule expiry on the outgoing snapshot — but never the root itself
    if let Some(ref prev_id) = current_latest_id {
        if prev_id != parent_id {
            let _: () = conn
                .expire(key_artifact(prev_id), STALE_SNAPSHOT_TTL_SECS)
                .await
                .map_err(|e| PcwError::RedisError(e.to_string()))?;
        }
    }

    // Advance the latest pointer
    let _: () = conn
        .set(key_artifact_latest(parent_id), &new_artifact.artifact_id)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    Ok(new_artifact)
}

/// List all version IDs for a base artifact.
pub async fn list_versions(
    base_artifact_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Vec<String>> {
    let ver_key = key_artifact_versions(base_artifact_id);
    conn.lrange(&ver_key, 0, -1)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))
}
