use redis::AsyncCommands;
use pcw_core::errors::{PcwError, PcwResult};
use pcw_core::models::DeltaShot;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};

pub const STREAM_PREFIX: &str = "pcw:deltashots";
pub const AUDIT_STREAM: &str  = "pcw:audit:log";

fn stream_key(session_id: &str) -> String {
    format!("{STREAM_PREFIX}:{session_id}")
}

/// XADD a DeltaShot. Binary fields stored as base64.
pub async fn append_shot(shot: &DeltaShot, conn: &mut redis::aio::MultiplexedConnection) -> PcwResult<String> {
    let key = stream_key(&shot.session_id);
    let diff_b64   = B64.encode(&shot.diff_payload);
    let hmac_b64   = B64.encode(&shot.hmac_signature);
    let artifacts  = serde_json::to_string(&shot.artifact_changes)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let meta       = serde_json::to_string(&shot.metadata)
        .map_err(|e| PcwError::SerializationError(e.to_string()))?;
    let agent_str  = shot.agent_type.as_ref().map(|a| a.to_string()).unwrap_or_default();
    let msg_idx    = shot.message_index.map(|i| i.to_string()).unwrap_or_default();
    let seq_str    = shot.sequence_number.to_string();
    let created_at = shot.created_at.to_rfc3339();

    let entry_id: String = conn.xadd::<_, _, _, _, String>(
        &key, "*",
        &[
            ("deltashot_id",    shot.deltashot_id.as_str()),
            ("session_id",      shot.session_id.as_str()),
            ("sequence_number", seq_str.as_str()),
            ("prev_hash",       shot.prev_hash.as_str()),
            ("content_hash",    shot.content_hash.as_str()),
            ("diff_payload",    diff_b64.as_str()),
            ("hmac_signature",  hmac_b64.as_str()),
            ("action",          shot.action.as_str()),
            ("agent_type",      agent_str.as_str()),
            ("message_index",   msg_idx.as_str()),
            ("artifact_changes",artifacts.as_str()),
            ("created_at",      created_at.as_str()),
            ("metadata",        meta.as_str()),
        ],
    ).await.map_err(|e| PcwError::RedisError(e.to_string()))?;

    Ok(entry_id)
}

/// XRANGE — read ALL shots for a session (ordered). Never uses XDEL.
pub async fn get_all_shots(session_id: &str, conn: &mut redis::aio::MultiplexedConnection) -> PcwResult<Vec<DeltaShot>> {
    let key = stream_key(session_id);
    let reply: redis::streams::StreamRangeReply = conn.xrange(&key, "-", "+").await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    let mut shots = Vec::new();
    for entry in reply.ids {
        shots.push(decode_shot(&entry.map)?);
    }
    Ok(shots)
}

/// XREVRANGE COUNT 1 — get the latest shot.
pub async fn get_latest_shot(session_id: &str, conn: &mut redis::aio::MultiplexedConnection) -> PcwResult<Option<DeltaShot>> {
    let key = stream_key(session_id);
    let reply: redis::streams::StreamRangeReply = conn.xrevrange_count(&key, "+", "-", 1usize).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    reply.ids.into_iter().next().map(|e| decode_shot(&e.map)).transpose()
}

/// XLEN — count shots.
pub async fn count_shots(session_id: &str, conn: &mut redis::aio::MultiplexedConnection) -> PcwResult<u64> {
    let key = stream_key(session_id);
    let count: usize = conn.xlen(&key).await.map_err(|e| PcwError::RedisError(e.to_string()))?;
    Ok(count as u64)
}

/// Append to the audit stream.
pub async fn append_audit(
    event_type: &str,
    session_id: &str,
    metadata_json: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    let timestamp = chrono::Utc::now().to_rfc3339();
    conn.xadd::<_, _, _, _, ()>(AUDIT_STREAM, "*", &[
        ("event_type", event_type),
        ("session_id", session_id),
        ("timestamp",  timestamp.as_str()),
        ("metadata",   metadata_json),
    ]).await.map_err(|e| PcwError::RedisError(e.to_string()))?;
    Ok(())
}

fn get_field(map: &std::collections::HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    }
}

fn decode_shot(map: &std::collections::HashMap<String, redis::Value>) -> PcwResult<DeltaShot> {
    use chrono::DateTime;
    let diff_b64    = get_field(map, "diff_payload");
    let hmac_b64    = get_field(map, "hmac_signature");
    let agent_str   = get_field(map, "agent_type");
    let msg_idx_s   = get_field(map, "message_index");
    let artifacts_s = get_field(map, "artifact_changes");
    let meta_s      = get_field(map, "metadata");
    let created_s   = get_field(map, "created_at");

    Ok(DeltaShot {
        deltashot_id:     get_field(map, "deltashot_id"),
        session_id:       get_field(map, "session_id"),
        sequence_number:  get_field(map, "sequence_number").parse().unwrap_or(0),
        prev_hash:        get_field(map, "prev_hash"),
        content_hash:     get_field(map, "content_hash"),
        diff_payload:     B64.decode(diff_b64).unwrap_or_default(),
        hmac_signature:   B64.decode(hmac_b64).unwrap_or_default(),
        action:           get_field(map, "action"),
        agent_type:       match agent_str.as_str() {
            "claude"   => Some(pcw_core::models::AgentType::Claude),
            "deepseek" => Some(pcw_core::models::AgentType::DeepSeek),
            _          => None,
        },
        message_index:    msg_idx_s.parse().ok(),
        artifact_changes: serde_json::from_str(&artifacts_s).unwrap_or_default(),
        created_at:       DateTime::parse_from_rfc3339(&created_s)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        metadata:         serde_json::from_str(&meta_s).unwrap_or_default(),
    })
}
