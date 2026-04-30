use redis::{aio::ConnectionManager, Client};
use pcw_core::errors::{PcwError, PcwResult};
use tokio::sync::OnceCell;

static MANAGER: OnceCell<ConnectionManager> = OnceCell::const_new();

/// Returns a clone of the ConnectionManager (cheap — it's an Arc internally).
pub async fn get_connection() -> PcwResult<ConnectionManager> {
    let mgr = MANAGER
        .get_or_try_init(|| async {
            let url = &pcw_core::config::get_settings().redis_url;
            let client = Client::open(url.as_str())
                .map_err(|e| PcwError::RedisError(e.to_string()))?;
            ConnectionManager::new(client)
                .await
                .map_err(|e| PcwError::RedisError(e.to_string()))
        })
        .await?;
    Ok(mgr.clone())
}

/// Creates a fresh MultiplexedConnection (no auto-reconnect, but correct type for deltashots).
pub async fn get_multiplexed_connection() -> PcwResult<redis::aio::MultiplexedConnection> {
    let url = &pcw_core::config::get_settings().redis_url;
    Client::open(url.as_str())
        .map_err(|e| PcwError::RedisError(e.to_string()))?
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))
}

/// Returns `true` if Redis responds to PING with PONG.
pub async fn ping() -> bool {
    match get_connection().await {
        Ok(mut conn) => {
            let result: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
            result.map(|s| s == "PONG").unwrap_or(false)
        }
        Err(_) => false,
    }
}

// ── Key namespace helpers ──────────────────────────────────────────────────

pub fn key_session_meta(session_id: &str) -> String {
    format!("pcw:session:{session_id}:meta")
}
pub fn key_workspace(workspace_id: &str) -> String {
    format!("pcw:workspace:{workspace_id}")
}
pub fn key_artifact(artifact_id: &str) -> String {
    format!("pcw:artifact:{artifact_id}")
}
pub fn key_artifact_versions(base_id: &str) -> String {
    format!("pcw:artifact:{base_id}:versions")
}
pub fn key_artifact_latest(root_id: &str) -> String {
    format!("pcw:artifact:{root_id}:latest")
}
pub fn key_workflow_state(workflow_id: &str) -> String {
    format!("pcw:workflow:{workflow_id}:state")
}
pub fn key_workflow_def(def_id: &str) -> String {
    format!("pcw:workflowdef:{def_id}")
}
pub fn key_scratchpad(session_id: &str) -> String {
    format!("pcw:scratchpad:{session_id}")
}
pub fn key_enc_key(session_id: &str) -> String {
    format!("pcw:key:{session_id}")
}
pub fn key_sign_key(session_id: &str) -> String {
    format!("pcw:key:{session_id}:signing")
}
pub fn key_session_artifacts(session_id: &str) -> String {
    format!("pcw:session:{session_id}:artifacts")
}
pub fn key_branch(branch_id: &str) -> String {
    format!("pcw:branch:{branch_id}")
}

pub const WORKFLOW_STREAM: &str = "pcw:workflow:steps";
pub const DEAD_LETTER_STREAM: &str = "pcw:workflow:dead-letter";
pub const AUDIT_STREAM: &str = "pcw:audit:log";
pub const CONSUMER_GROUP: &str = "pcw-workers";
