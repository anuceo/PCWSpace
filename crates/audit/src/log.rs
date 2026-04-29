use pcw_core::errors::{PcwError, PcwResult};
use redis::AsyncCommands;
use serde_json::json;
use tracing::debug;

pub const AUDIT_STREAM: &str = "pcw:audit:log";

pub async fn record(
    event_type: &str,
    session_id: Option<&str>,
    actor: &str,
    detail: serde_json::Value,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    let timestamp = chrono::Utc::now().to_rfc3339();
    let detail_str = detail.to_string();
    let sid = session_id.unwrap_or("");

    debug!(event_type, actor, "Recording audit event");

    let _: String = conn
        .xadd(
            AUDIT_STREAM,
            "*",
            &[
                ("event_type", event_type),
                ("session_id", sid),
                ("actor", actor),
                ("timestamp", timestamp.as_str()),
                ("detail", detail_str.as_str()),
            ],
        )
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    Ok(())
}

pub async fn record_session_created(
    session_id: &str,
    workspace_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    record(
        "SESSION_CREATED",
        Some(session_id),
        "api",
        json!({"workspace_id": workspace_id}),
        conn,
    )
    .await
}

pub async fn record_agent_called(
    session_id: &str,
    agent: &str,
    tokens: u32,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    record(
        "AGENT_CALLED",
        Some(session_id),
        "orchestrator",
        json!({"agent": agent, "tokens_used": tokens}),
        conn,
    )
    .await
}
