use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use pcw_core::{errors::{PcwError, PcwResult}, models::{DeltaShot, AgentType, new_id, now}};
use serde_json::Value;
use std::collections::HashMap;
use crate::{diff, encryption, hash, store};

const ENC_KEY_PREFIX:  &str = "pcw:key";
const SIGN_KEY_SUFFIX: &str = ":signing";

async fn get_or_create_key(redis_key: &str, ttl: u64, conn: &mut MultiplexedConnection) -> PcwResult<Vec<u8>> {
    let raw: Option<Vec<u8>> = conn.get(redis_key).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    if let Some(k) = raw {
        // Slide the TTL window on every access so active sessions never expire mid-use
        let _: () = conn.expire(redis_key, ttl as i64).await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        return Ok(k);
    }
    let new_key = encryption::generate_key();
    let _: () = conn.set_ex(redis_key, new_key.as_slice(), ttl).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    Ok(new_key)
}

pub struct AppendParams<'a> {
    pub session_id: &'a str,
    pub before: Value,
    pub after: Value,
    pub action: &'a str,
    pub agent_type: Option<AgentType>,
    pub message_index: Option<u64>,
    pub artifact_changes: Vec<String>,
    pub metadata: HashMap<String, Value>,
}

pub async fn append_deltashot(
    params: AppendParams<'_>,
    conn: &mut MultiplexedConnection,
) -> PcwResult<DeltaShot> {
    let ttl = pcw_core::config::get_settings().session_key_ttl_secs;
    let session_id = params.session_id;

    // 1. Compute diff
    let diff_val  = diff::compute_diff(&params.before, &params.after);
    let plaintext = diff::diff_to_bytes(&diff_val);

    // 2. Get/create encryption key
    let enc_key_redis = format!("{ENC_KEY_PREFIX}:{session_id}");
    let enc_key = get_or_create_key(&enc_key_redis, ttl, conn).await?;

    // 3. Encrypt
    let encrypted = encryption::encrypt(&plaintext, &enc_key)?;

    // 4. Sequence number + prev_hash
    let latest = store::get_latest_shot(session_id, conn).await?;
    let sequence_number = latest.as_ref().map(|s| s.sequence_number + 1).unwrap_or(0);
    let prev_hash = latest.as_ref().map(|s| s.content_hash.as_str()).unwrap_or("").to_string();

    // 5. Content hash: SHA-256(prev_hash || encrypted)
    let content_hash = hash::compute_content_hash(&prev_hash, &encrypted);

    // 6. Signing key + HMAC
    let sign_key_redis = format!("{ENC_KEY_PREFIX}:{session_id}{SIGN_KEY_SUFFIX}");
    let sign_key = get_or_create_key(&sign_key_redis, ttl, conn).await?;
    let hmac_sig = hash::hmac_sign(&content_hash, &sign_key);

    // 7. Build shot
    let shot = DeltaShot {
        deltashot_id:     new_id(),
        session_id:       session_id.to_string(),
        sequence_number,
        prev_hash,
        content_hash,
        diff_payload:     encrypted,
        hmac_signature:   hmac_sig,
        action:           params.action.to_string(),
        agent_type:       params.agent_type,
        message_index:    params.message_index,
        artifact_changes: params.artifact_changes,
        created_at:       now(),
        metadata:         params.metadata,
    };

    // 8. Persist (append-only)
    store::append_shot(&shot, conn).await?;

    // 9. Audit log
    let meta = serde_json::json!({
        "deltashot_id": &shot.deltashot_id,
        "action": params.action,
        "sequence_number": sequence_number,
    });
    store::append_audit(
        "DELTASHOT_APPENDED", session_id, &meta.to_string(), conn,
    ).await?;

    Ok(shot)
}

pub async fn get_decrypted_diff(shot: &DeltaShot, conn: &mut MultiplexedConnection) -> PcwResult<Value> {
    let ttl = pcw_core::config::get_settings().session_key_ttl_secs;
    let enc_key_redis = format!("{ENC_KEY_PREFIX}:{}", shot.session_id);
    let enc_key: Option<Vec<u8>> = conn.get(&enc_key_redis).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let enc_key = enc_key.ok_or_else(|| PcwError::EncryptionKeyNotFound(shot.session_id.clone()))?;
    // Slide the TTL on reads too — a long replay must not expire the key mid-stream
    let _: () = conn.expire(&enc_key_redis, ttl as i64).await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    let plaintext = encryption::decrypt(&shot.diff_payload, &enc_key)?;
    diff::bytes_to_diff(&plaintext)
}
