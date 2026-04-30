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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{diff, encryption, hash};
    use pcw_core::{
        errors::PcwError,
        models::{DeltaShot, new_id, now},
    };
    use serde_json::json;
    use std::collections::HashMap;

    // ── helpers ───────────────────────────────────────────────────────────

    fn build_shot(
        session_id: &str,
        seq: u64,
        prev_hash: &str,
        diff_val: &Value,
        enc_key: &[u8],
        sign_key: &[u8],
    ) -> DeltaShot {
        let plaintext = diff::diff_to_bytes(diff_val);
        let encrypted = encryption::encrypt(&plaintext, enc_key).unwrap();
        let content_hash = hash::compute_content_hash(prev_hash, &encrypted);
        let hmac_sig = hash::hmac_sign(&content_hash, sign_key);
        DeltaShot {
            deltashot_id: new_id(),
            session_id: session_id.to_string(),
            sequence_number: seq,
            prev_hash: prev_hash.to_string(),
            content_hash,
            diff_payload: encrypted,
            hmac_signature: hmac_sig,
            action: "TEST".to_string(),
            agent_type: None,
            message_index: None,
            artifact_changes: vec![],
            created_at: now(),
            metadata: HashMap::new(),
        }
    }

    /// Pure replay without Redis — mirrors the inner loop of `replay_session`.
    fn run_replay(
        shots: &[DeltaShot],
        enc_key: &[u8],
        sign_key: &[u8],
    ) -> PcwResult<Value> {
        let mut state = json!({});
        let mut expected_prev = String::new();

        for shot in shots {
            if shot.prev_hash != expected_prev {
                return Err(PcwError::TamperDetected {
                    shot_id: shot.deltashot_id.clone(),
                    reason: format!("prev_hash mismatch at seq {}", shot.sequence_number),
                });
            }
            if !hash::verify_chain(&shot.prev_hash, &shot.diff_payload, &shot.content_hash) {
                return Err(PcwError::TamperDetected {
                    shot_id: shot.deltashot_id.clone(),
                    reason: format!("content hash mismatch at seq {}", shot.sequence_number),
                });
            }
            if !hash::hmac_verify(&shot.content_hash, &shot.hmac_signature, sign_key) {
                return Err(PcwError::TamperDetected {
                    shot_id: shot.deltashot_id.clone(),
                    reason: format!("HMAC failed at seq {}", shot.sequence_number),
                });
            }
            let plaintext = encryption::decrypt(&shot.diff_payload, enc_key)?;
            let d = diff::bytes_to_diff(&plaintext)?;
            if !diff::is_diff_empty(&d) {
                diff::apply_diff(&mut state, &d);
            }
            expected_prev = shot.content_hash.clone();
        }
        Ok(state)
    }

    // ── correctness ───────────────────────────────────────────────────────

    #[test]
    fn empty_chain_returns_empty_state() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();
        assert_eq!(run_replay(&[], &enc, &sign).unwrap(), json!({}));
    }

    #[test]
    fn single_shot_reconstructs_state() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();
        let d0 = diff::compute_diff(&json!({}), &json!({"name": "Alice"}));
        let s0 = build_shot("sess", 0, "", &d0, &enc, &sign);
        assert_eq!(run_replay(&[s0], &enc, &sign).unwrap(), json!({"name": "Alice"}));
    }

    #[test]
    fn multi_shot_chain_accumulates_state() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();

        let d0 = diff::compute_diff(&json!({}), &json!({"name": "Alice"}));
        let s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        let d1 = diff::compute_diff(&json!({"name": "Alice"}), &json!({"name": "Alice", "score": 10}));
        let s1 = build_shot("s", 1, &s0.content_hash, &d1, &enc, &sign);

        let d2 = diff::compute_diff(
            &json!({"name": "Alice", "score": 10}),
            &json!({"name": "Alice", "score": 20}),
        );
        let s2 = build_shot("s", 2, &s1.content_hash, &d2, &enc, &sign);

        let state = run_replay(&[s0, s1, s2], &enc, &sign).unwrap();
        assert_eq!(state, json!({"name": "Alice", "score": 20}));
    }

    #[test]
    fn partial_replay_stops_at_given_sequence() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();

        let d0 = diff::compute_diff(&json!({}), &json!({"step": 0}));
        let s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        let d1 = diff::compute_diff(&json!({"step": 0}), &json!({"step": 1}));
        let s1 = build_shot("s", 1, &s0.content_hash, &d1, &enc, &sign);

        let d2 = diff::compute_diff(&json!({"step": 1}), &json!({"step": 2}));
        let s2 = build_shot("s", 2, &s1.content_hash, &d2, &enc, &sign);

        // Feed only shots 0 and 1 — equivalent to up_to_sequence = 1
        let state = run_replay(&[s0, s1], &enc, &sign).unwrap();
        assert_eq!(state["step"], json!(1));

        // Shot 2 exists but was not replayed
        drop(s2);
    }

    // ── tamper detection ──────────────────────────────────────────────────

    #[test]
    fn tampered_payload_detected() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();
        let d0 = diff::compute_diff(&json!({}), &json!({"x": 1}));
        let mut s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        // Flip a byte in the ciphertext — content hash will no longer match
        s0.diff_payload[encryption::IV_LEN] ^= 0xFF;

        let err = run_replay(&[s0], &enc, &sign).unwrap_err();
        assert!(matches!(err, PcwError::TamperDetected { .. }));
    }

    #[test]
    fn tampered_prev_hash_detected() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();
        let d0 = diff::compute_diff(&json!({}), &json!({"a": 1}));
        let s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        let d1 = diff::compute_diff(&json!({"a": 1}), &json!({"a": 2}));
        let mut s1 = build_shot("s", 1, &s0.content_hash, &d1, &enc, &sign);

        // Break the chain link
        s1.prev_hash = "not_the_right_hash".to_string();

        let err = run_replay(&[s0, s1], &enc, &sign).unwrap_err();
        assert!(matches!(err, PcwError::TamperDetected { .. }));
    }

    #[test]
    fn tampered_hmac_detected() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();
        let d0 = diff::compute_diff(&json!({}), &json!({"val": "hello"}));
        let mut s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        // Corrupt the HMAC signature
        s0.hmac_signature[0] ^= 0xFF;

        let err = run_replay(&[s0], &enc, &sign).unwrap_err();
        assert!(matches!(err, PcwError::TamperDetected { .. }));
    }

    #[test]
    fn wrong_decryption_key_caught() {
        let enc      = encryption::generate_key();
        let wrong    = encryption::generate_key();
        let sign     = encryption::generate_key();
        let d0 = diff::compute_diff(&json!({}), &json!({"secret": "data"}));
        let s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        // HMAC check uses sign_key (correct), but decryption uses wrong enc key
        // The HMAC passes (we use correct sign key), decryption itself fails
        // with AES-GCM auth error, which surfaces as EncryptionFailed
        let result = run_replay(&[s0], &wrong, &sign);
        assert!(result.is_err());
    }

    #[test]
    fn gap_in_chain_detected() {
        let enc  = encryption::generate_key();
        let sign = encryption::generate_key();

        let d0 = diff::compute_diff(&json!({}), &json!({"a": 1}));
        let s0 = build_shot("s", 0, "", &d0, &enc, &sign);

        // Build s2 referencing s0 directly (skipping s1 — orphaned hash)
        let d2 = diff::compute_diff(&json!({"a": 1}), &json!({"a": 3}));
        // Intentionally use wrong prev_hash (simulate a gap)
        let s2 = build_shot("s", 2, "wrong_prev_hash", &d2, &enc, &sign);

        let err = run_replay(&[s0, s2], &enc, &sign).unwrap_err();
        assert!(matches!(err, PcwError::TamperDetected { .. }));
    }
}
