use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::apply::apply_ops_to_state;
use crate::diff::compute_diff_ops;
use crate::ops::{canonical_serialize_ops, DeltaShot, Op};

#[derive(Default)]
pub struct DeltaShotEngine;

impl DeltaShotEngine {
    pub fn build_delta(&self, base: &Value, next: &Value) -> Vec<Op> {
        compute_diff_ops(base, next)
    }

    pub fn apply_delta(&self, base: &Value, ops: &[Op]) -> Result<Value> {
        apply_ops_to_state(base, ops)
    }
}

pub fn compute_chain_hash(prev_hash: &str, canonical_ops: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(canonical_ops);
    hasher.finalize().to_hex().to_string()
}

pub fn verify_hash_chain(deltas: &[DeltaShot]) -> Result<()> {
    for i in 1..deltas.len() {
        let prev = &deltas[i - 1];
        let current = &deltas[i];
        let canonical = canonical_serialize_ops(&current.ops)?;
        let recomputed = compute_chain_hash(&prev.hash, &canonical);
        if recomputed != current.hash {
            return Err(anyhow!("hash chain broken at {}", current.id));
        }
        if current.prev_hash != prev.hash {
            return Err(anyhow!("prev hash link mismatch at {}", current.id));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compute_chain_hash, verify_hash_chain};
    use crate::ops::{canonical_serialize_ops, DeltaShot, Metadata, Op, OpType};

    #[test]
    fn verifies_chain_integrity() {
        let first_ops = vec![Op::new(
            "/goal".to_owned(),
            OpType::Set,
            Some(json!("draft")),
        )];
        let first_hash = compute_chain_hash(
            "",
            &canonical_serialize_ops(&first_ops).expect("canonical should serialize"),
        );
        let first = DeltaShot {
            id: "ds_1".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: "".to_owned(),
            hash: first_hash.clone(),
            timestamp: 1,
            ops: first_ops,
            artifacts: Vec::new(),
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };

        let second_ops = vec![Op::new(
            "/goal".to_owned(),
            OpType::Replace,
            Some(json!("final")),
        )];
        let second_hash = compute_chain_hash(
            &first_hash,
            &canonical_serialize_ops(&second_ops).expect("canonical should serialize"),
        );
        let second = DeltaShot {
            id: "ds_2".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: first_hash,
            hash: second_hash,
            timestamp: 2,
            ops: second_ops,
            artifacts: Vec::new(),
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };

        verify_hash_chain(&[first, second]).expect("chain should validate");
    }
}
