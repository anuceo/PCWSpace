use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{Session, new_id, now},
};
use deltashots::{replay::replay_session, engine::{append_deltashot, AppendParams}};
use infra::redis_client::{key_session_meta, key_branch};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchInfo {
    pub branch_id: String,
    pub source_session_id: String,
    pub fork_at_sequence: u64,
    pub new_session_id: String,
    pub created_at: String,
}

/// Fork a session at a specific DeltaShot sequence number into a new session.
#[instrument(skip(conn))]
pub async fn fork_session(
    source_session_id: &str,
    fork_at_sequence: u64,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Session> {
    // Replay state up to the fork point
    let replayed_state = replay_session(source_session_id, Some(fork_at_sequence), conn).await?;

    // Load source session metadata
    let meta_key = key_session_meta(source_session_id);
    let raw: Option<String> = conn
        .get(&meta_key)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let source: Session = raw
        .ok_or_else(|| PcwError::SessionNotFound(source_session_id.to_string()))
        .and_then(|s| {
            serde_json::from_str(&s).map_err(|e| PcwError::SerializationError(e.to_string()))
        })?;

    // Create forked session
    let mut forked = Session::new(source.workspace_id.clone());
    forked.metadata.insert(
        "forked_from".into(),
        serde_json::Value::String(source_session_id.to_string()),
    );
    forked.metadata.insert(
        "fork_at_sequence".into(),
        serde_json::Value::Number(fork_at_sequence.into()),
    );

    // Persist the new session
    let new_key = key_session_meta(&forked.session_id);
    let json = serde_json::to_string(&forked)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let _: () = conn
        .set(&new_key, json)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    // Write initial DeltaShot representing the fork point state
    append_deltashot(
        AppendParams {
            session_id: &forked.session_id,
            before: serde_json::Value::Object(Default::default()),
            after: replayed_state,
            action: "BRANCH_FORK",
            agent_type: None,
            message_index: None,
            artifact_changes: vec![],
            metadata: {
                let mut m = std::collections::HashMap::new();
                m.insert("source_session_id".into(), serde_json::Value::String(source_session_id.to_string()));
                m.insert("fork_at_sequence".into(), serde_json::Value::Number(fork_at_sequence.into()));
                m
            },
        },
        conn,
    )
    .await?;

    // Record branch metadata
    let branch_info = BranchInfo {
        branch_id: new_id(),
        source_session_id: source_session_id.to_string(),
        fork_at_sequence,
        new_session_id: forked.session_id.clone(),
        created_at: now().to_rfc3339(),
    };
    let branch_key = key_branch(&branch_info.branch_id);
    let branch_json = serde_json::to_string(&branch_info)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let _: () = conn
        .set(&branch_key, branch_json)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    Ok(forked)
}
