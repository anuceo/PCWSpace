use pcw_core::errors::{PcwError, PcwResult};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub stream_id: String,
    pub event_type: String,
    pub session_id: String,
    pub actor: String,
    pub timestamp: String,
    pub detail: String,
}

pub async fn list_recent(
    count: usize,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Vec<AuditEntry>> {
    let reply: redis::streams::StreamRangeReply = conn
        .xrevrange_count(crate::log::AUDIT_STREAM, "+", "-", count)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    let entries = reply
        .ids
        .into_iter()
        .map(|e| decode_entry(e.id, &e.map))
        .collect();
    Ok(entries)
}

pub async fn list_for_session(
    session_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Vec<AuditEntry>> {
    let all = list_recent(10_000, conn).await?;
    Ok(all.into_iter().filter(|e| e.session_id == session_id).collect())
}

fn get_field(map: &HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    }
}

fn decode_entry(id: String, map: &HashMap<String, redis::Value>) -> AuditEntry {
    AuditEntry {
        stream_id: id,
        event_type: get_field(map, "event_type"),
        session_id: get_field(map, "session_id"),
        actor: get_field(map, "actor"),
        timestamp: get_field(map, "timestamp"),
        detail: get_field(map, "detail"),
    }
}
