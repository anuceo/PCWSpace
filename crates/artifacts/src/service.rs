use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{Artifact, ArtifactType, AgentType, new_id, now},
};
use infra::{metrics, redis_client::{key_artifact, key_artifact_versions, key_artifact_latest, key_session_artifacts}};
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use tracing::{debug, instrument};

pub struct ArtifactService;

impl ArtifactService {
    #[instrument(skip(conn))]
    pub async fn create(
        name: &str,
        artifact_type: ArtifactType,
        content: &str,
        session_id: Option<&str>,
        agent_type: Option<AgentType>,
        deltashot_id: Option<&str>,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<Artifact> {
        let content_hash = hex::encode(Sha256::digest(content.as_bytes()));

        let artifact = Artifact {
            artifact_id: new_id(),
            name: name.to_string(),
            artifact_type,
            content: content.to_string(),
            content_hash,
            version: 1,
            parent_version_id: None,
            linked_session: session_id.map(str::to_string),
            agent_type,
            deltashot_id: deltashot_id.map(str::to_string),
            created_at: now(),
            metadata: Default::default(),
        };

        Self::persist(&artifact, conn).await?;

        // Initialise the latest pointer — on a fresh artifact v1 is the latest
        let _: () = conn
            .set(key_artifact_latest(&artifact.artifact_id), &artifact.artifact_id)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        if let Some(sid) = session_id {
            // Issue a baton binding this chain to its originating session
            crate::baton::init(&artifact.artifact_id, sid, conn).await?;

            let _: () = conn
                .sadd(key_session_artifacts(sid), &artifact.artifact_id)
                .await
                .map_err(|e| PcwError::RedisError(e.to_string()))?;
        }

        metrics::global().increment(metrics::names::ARTIFACTS_CREATED);
        debug!(artifact_id = %artifact.artifact_id, "Artifact created");
        Ok(artifact)
    }

    pub async fn get(
        artifact_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<Artifact> {
        // Resolve to root ID: if this artifact has a parent, its root is the parent
        // (all chains are one level deep — parent_version_id always points to root).
        // Check the dropped set using the root to cover every version in the chain.
        let root_id_check = {
            let key = key_artifact(artifact_id);
            let raw: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| PcwError::RedisError(e.to_string()))?;
            let json = raw.ok_or_else(|| PcwError::ArtifactNotFound(artifact_id.to_string()))?;
            let a: Artifact = serde_json::from_str(&json)
                .map_err(|e| PcwError::SerializationError(e.to_string()))?;
            let root = a.parent_version_id.clone().unwrap_or_else(|| artifact_id.to_string());
            (a, root)
        };
        let (artifact, root_id) = root_id_check;

        if crate::baton::is_dropped(&root_id, conn).await? {
            return Err(PcwError::BatonDropped(format!(
                "artifact chain {root_id} is sealed"
            )));
        }

        Ok(artifact)
    }

    pub async fn list_for_session(
        session_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<Vec<Artifact>> {
        // The set contains only root IDs — one entry per logical artifact
        let root_ids: Vec<String> = conn
            .smembers(key_session_artifacts(session_id))
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        let mut artifacts = Vec::new();
        for root_id in root_ids {
            // Resolve to the current version via the latest pointer
            let latest_id: Option<String> = conn
                .get(key_artifact_latest(&root_id))
                .await
                .unwrap_or(None);
            let resolve_id = latest_id.as_deref().unwrap_or(root_id.as_str());
            if let Ok(a) = Self::get(resolve_id, conn).await {
                artifacts.push(a);
            }
        }
        artifacts.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(artifacts)
    }

    async fn persist(
        artifact: &Artifact,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let key = key_artifact(&artifact.artifact_id);
        let json =
            serde_json::to_string(artifact).map_err(|e| PcwError::SerializationError(e.to_string()))?;
        let _: () = conn
            .set(&key, json)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        // Track in version list
        let ver_key = key_artifact_versions(&artifact.artifact_id);
        let _: () = conn
            .rpush(&ver_key, &artifact.artifact_id)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        Ok(())
    }
}
