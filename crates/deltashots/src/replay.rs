use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde_json::Value;
use pcw_core::errors::{PcwError, PcwResult};
use crate::{diff, encryption, hash, store};

pub async fn replay_session(
    session_id: &str,
    up_to_sequence: Option<u64>,
    conn: &mut MultiplexedConnection,
) -> PcwResult<Value> {
    let all_shots = store::get_all_shots(session_id, conn).await?;
    let shots: Vec<_> = match up_to_sequence {
        Some(n) => all_shots.into_iter().filter(|s| s.sequence_number <= n).collect(),
        None    => all_shots,
    };
    if shots.is_empty() { return Ok(Value::Object(Default::default())); }

    // Load keys
    let enc_key_redis  = format!("pcw:key:{session_id}");
    let sign_key_redis = format!("pcw:key:{session_id}:signing");
    let enc_key: Option<Vec<u8>>  = conn.get(&enc_key_redis).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let sign_key: Option<Vec<u8>> = conn.get(&sign_key_redis).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let enc_key  = enc_key.ok_or_else(|| PcwError::EncryptionKeyNotFound(session_id.to_string()))?;
    let sign_key = sign_key.ok_or_else(|| PcwError::EncryptionKeyNotFound(session_id.to_string()))?;

    let mut state = Value::Object(Default::default());
    let mut expected_prev_hash = String::new();

    for shot in &shots {
        // 1. Chain integrity
        if shot.prev_hash != expected_prev_hash {
            return Err(PcwError::TamperDetected {
                shot_id: shot.deltashot_id.clone(),
                reason: format!(
                    "expected prev_hash={:?} got={:?} at seq {}",
                    expected_prev_hash, shot.prev_hash, shot.sequence_number
                ),
            });
        }
        // 2. Content hash
        if !hash::verify_chain(&shot.prev_hash, &shot.diff_payload, &shot.content_hash) {
            return Err(PcwError::TamperDetected {
                shot_id: shot.deltashot_id.clone(),
                reason: format!("content hash mismatch at seq {}", shot.sequence_number),
            });
        }
        // 3. HMAC
        if !hash::hmac_verify(&shot.content_hash, &shot.hmac_signature, &sign_key) {
            return Err(PcwError::TamperDetected {
                shot_id: shot.deltashot_id.clone(),
                reason: format!("HMAC verification failed at seq {}", shot.sequence_number),
            });
        }
        // 4. Decrypt
        let plaintext = encryption::decrypt(&shot.diff_payload, &enc_key)?;
        // 5. Apply diff
        let d = diff::bytes_to_diff(&plaintext)?;
        if !diff::is_diff_empty(&d) {
            diff::apply_diff(&mut state, &d);
        }
        expected_prev_hash = shot.content_hash.clone();
    }

    Ok(state)
}

pub async fn list_rollback_points(
    session_id: &str,
    conn: &mut MultiplexedConnection,
) -> PcwResult<Vec<serde_json::Value>> {
    let shots = store::get_all_shots(session_id, conn).await?;
    Ok(shots.into_iter().map(|s| serde_json::json!({
        "deltashot_id": s.deltashot_id,
        "sequence_number": s.sequence_number,
        "action": s.action,
        "agent_type": s.agent_type,
        "message_index": s.message_index,
        "artifact_changes": s.artifact_changes,
        "created_at": s.created_at.to_rfc3339(),
        "content_hash": s.content_hash,
    })).collect())
}
