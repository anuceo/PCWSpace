use pcw_core::errors::{PcwError, PcwResult};
use infra::redis_client::{
    key_artifact_baton, key_artifact_latest, key_artifact_versions, BATON_DROPPED_SET,
};
use redis::AsyncCommands;

/// Ephemeral TTL applied to chain-metadata keys when a baton is dropped.
/// Gives in-flight reads a 1-hour grace window before chain keys vanish.
const EPHEMERAL_TTL_SECS: i64 = 3600;

// ── Public API ────────────────────────────────────────────────────────────────

/// Bind a baton to a root artifact at creation time.
/// Records `session_id` as the sole authorised connection point for this chain.
pub async fn init(
    root_id: &str,
    session_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    let _: () = conn
        .set(key_artifact_baton(root_id), session_id)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    Ok(())
}

/// Verify that `session_id` matches the stored baton before any write.
///
/// - If the chain is already in the dropped set → immediate error (permanent).
/// - If the stored session differs from the caller's → drop the baton, then error.
/// - If no baton is stored (artifact created without a session) → pass through.
pub async fn verify(
    root_id: &str,
    session_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    if is_dropped(root_id, conn).await? {
        return Err(PcwError::BatonDropped(format!(
            "artifact chain {root_id} is sealed — no further writes accepted"
        )));
    }

    let bound: Option<String> = conn
        .get(key_artifact_baton(root_id))
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    let bound_session = match bound {
        Some(s) => s,
        None => return Ok(()), // unlinked artifact — no baton enforced
    };

    if bound_session != session_id {
        drop_baton(root_id, conn).await?;
        return Err(PcwError::BatonDropped(format!(
            "connection point mismatch for artifact {root_id} \
             (expected session {bound_session}, got {session_id}) — baton dropped, chain sealed"
        )));
    }

    Ok(())
}

/// Returns true if this root artifact's baton has been dropped.
pub async fn is_dropped(
    root_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<bool> {
    conn.sismember(BATON_DROPPED_SET, root_id)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))
}

/// Seal the artifact chain permanently.
///
/// 1. Adds `root_id` to the permanent deny set (no TTL — survives restarts).
/// 2. Sets a 1-hour ephemeral TTL on all chain-navigation keys so they
///    self-destruct: `latest` pointer, `versions` list, and the `baton` itself.
///
/// Individual version artifact payloads are not expired here — they keep
/// whatever TTL the stale-snapshot logic already assigned them (stale ones
/// expire in 7 days; the root and the former latest have no TTL). They become
/// unreachable via normal navigation once the chain keys expire.
pub async fn drop_baton(
    root_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<()> {
    // Permanent deny — SADD has no TTL
    let _: () = conn
        .sadd(BATON_DROPPED_SET, root_id)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    // Seal chain-navigation metadata
    for key in [
        key_artifact_baton(root_id),
        key_artifact_latest(root_id),
        key_artifact_versions(root_id),
    ] {
        let _: () = conn
            .expire(&key, EPHEMERAL_TTL_SECS)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
    }

    Ok(())
}
