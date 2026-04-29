use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{Artifact, AgentType, new_id, now},
};
use infra::redis_client::{key_artifact, key_artifact_versions};
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use tracing::instrument;

/// Create a new version of an existing artifact.
#[instrument(skip(conn))]
pub async fn new_version(
    parent_id: &str,
    new_content: &str,
    agent_type: Option<AgentType>,
    deltashot_id: Option<&str>,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Artifact> {
    let parent_key = key_artifact(parent_id);
    let raw: Option<String> = conn
        .get(&parent_key)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let parent: Artifact = raw
        .ok_or_else(|| PcwError::ArtifactNotFound(parent_id.to_string()))
        .and_then(|s| {
            serde_json::from_str(&s).map_err(|e| PcwError::SerializationError(e.to_string()))
        })?;

    let content_hash = hex::encode(Sha256::digest(new_content.as_bytes()));

    let new_artifact = Artifact {
        artifact_id: new_id(),
        name: parent.name.clone(),
        artifact_type: parent.artifact_type.clone(),
        content: new_content.to_string(),
        content_hash,
        version: parent.version + 1,
        parent_version_id: Some(parent_id.to_string()),
        linked_session: parent.linked_session.clone(),
        agent_type,
        deltashot_id: deltashot_id.map(str::to_string),
        created_at: now(),
        metadata: Default::default(),
    };

    let new_key = key_artifact(&new_artifact.artifact_id);
    let json = serde_json::to_string(&new_artifact)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let _: () = conn
        .set(&new_key, json)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    // Append to version list of the original artifact chain
    let ver_key = key_artifact_versions(parent_id);
    let _: () = conn
        .rpush(&ver_key, &new_artifact.artifact_id)
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
