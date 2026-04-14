use std::collections::HashMap;
use std::io::Read;
use std::sync::RwLock;

use anyhow::Result;
use deltashot::{apply_ops, canonical_serialize_ops, compute_chain_hash, DeltaShot, Op};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub valid: bool,
    pub verified_count: usize,
    pub failed_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayAuditResult {
    pub valid: bool,
    pub final_state_match: bool,
    pub hash_match: bool,
}

pub trait DeltaRepository {
    fn load_branch_chain(
        &self,
        branch_id: &str,
    ) -> impl std::future::Future<Output = Result<Vec<DeltaShot>>> + Send;
    fn load_ops(
        &self,
        deltashot_id: &str,
    ) -> impl std::future::Future<Output = Result<Vec<Op>>> + Send;
    fn load_compressed_ops(
        &self,
        deltashot_id: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;
}

pub trait StateRepository {
    fn get_branch_state(
        &self,
        branch_id: &str,
    ) -> impl std::future::Future<Output = Result<Value>> + Send;
    fn load_nearest_snapshot(
        &self,
        branch_id: &str,
    ) -> impl std::future::Future<Output = Result<(Value, usize)>> + Send;
}

pub async fn verify_chain(
    delta_repo: &impl DeltaRepository,
    branch_id: &str,
) -> Result<VerificationResult> {
    let deltas = delta_repo.load_branch_chain(branch_id).await?;

    if deltas.is_empty() {
        return Ok(VerificationResult {
            valid: true,
            verified_count: 0,
            failed_at: None,
            error: None,
        });
    }

    for i in 1..deltas.len() {
        let prev = &deltas[i - 1];
        let current = &deltas[i];

        let ops = delta_repo.load_ops(&current.id).await?;
        let canonical = canonical_serialize_ops(&ops)?;
        let recomputed = compute_chain_hash(&prev.hash, &canonical);

        if recomputed != current.hash {
            error!("Mismatch at DeltaShot {}", current.id);
            debug!("Expected hash: {}, got: {}", current.hash, recomputed);
            return Ok(VerificationResult {
                valid: false,
                verified_count: i,
                failed_at: Some(current.id.clone()),
                error: Some("Hash mismatch".to_owned()),
            });
        }

        if current.prev_hash != prev.hash {
            error!("Mismatch at DeltaShot {}", current.id);
            debug!(
                "Expected prev_hash: {}, got: {}",
                prev.hash, current.prev_hash
            );
            return Ok(VerificationResult {
                valid: false,
                verified_count: i,
                failed_at: Some(current.id.clone()),
                error: Some("Prev-hash link mismatch".to_owned()),
            });
        }
    }

    Ok(VerificationResult {
        valid: true,
        verified_count: deltas.len(),
        failed_at: None,
        error: None,
    })
}

pub async fn audit_replay(
    delta_repo: &impl DeltaRepository,
    state_repo: &impl StateRepository,
    branch_id: &str,
) -> Result<ReplayAuditResult> {
    let deltas = delta_repo.load_branch_chain(branch_id).await?;

    let (mut state, start_index) = state_repo.load_nearest_snapshot(branch_id).await?;
    if start_index > deltas.len() {
        anyhow::bail!(
            "snapshot start index {} out of range for {} deltas",
            start_index,
            deltas.len()
        );
    }

    let mut last_hash = if start_index == 0 {
        String::new()
    } else {
        deltas[start_index - 1].hash.clone()
    };

    for ds in deltas.iter().skip(start_index) {
        let compressed = delta_repo.load_compressed_ops(&ds.id).await?;
        let decompressed = decompress_ops(&compressed)?;
        let ops: Vec<Op> = serde_json::from_slice(&decompressed)?;

        apply_ops(&mut state, &ops)?;

        let canonical = canonical_serialize_ops(&ops)?;
        let recomputed = compute_chain_hash(&last_hash, &canonical);

        if recomputed != ds.hash || (!last_hash.is_empty() && ds.prev_hash != last_hash) {
            error!("Mismatch at DeltaShot {}", ds.id);
            debug!(
                "Expected hash: {}, got: {}; expected prev: {}, got: {}",
                ds.hash, recomputed, last_hash, ds.prev_hash
            );
            return Ok(ReplayAuditResult {
                valid: false,
                final_state_match: false,
                hash_match: false,
            });
        }

        last_hash = recomputed;
    }

    let stored = state_repo.get_branch_state(branch_id).await?;
    let final_state_match = state == stored;

    Ok(ReplayAuditResult {
        valid: final_state_match,
        final_state_match,
        hash_match: true,
    })
}

pub async fn full_audit(
    delta_repo: &impl DeltaRepository,
    state_repo: &impl StateRepository,
    branch_id: &str,
) -> Result<()> {
    let chain = verify_chain(delta_repo, branch_id).await?;
    if !chain.valid {
        anyhow::bail!("Chain verification failed at {:?}", chain.failed_at);
    }

    let replay = audit_replay(delta_repo, state_repo, branch_id).await?;
    if !replay.valid {
        anyhow::bail!("Replay mismatch detected");
    }

    Ok(())
}

pub fn decompress_ops(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decompressor = brotli::Decompressor::new(compressed, 4096);
    let mut decompressed = Vec::new();
    decompressor.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

#[derive(Debug, Default)]
pub struct InMemoryDeltaRepository {
    branch_chains: RwLock<HashMap<String, Vec<DeltaShot>>>,
    ops: RwLock<HashMap<String, Vec<Op>>>,
    compressed_ops: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryDeltaRepository {
    pub fn insert_branch_chain(&self, branch_id: &str, deltas: Vec<DeltaShot>) {
        self.branch_chains
            .write()
            .expect("lock should not be poisoned")
            .insert(branch_id.to_owned(), deltas);
    }

    pub fn insert_ops(&self, delta_id: &str, ops: Vec<Op>, compressed: Vec<u8>) {
        self.ops
            .write()
            .expect("lock should not be poisoned")
            .insert(delta_id.to_owned(), ops);
        self.compressed_ops
            .write()
            .expect("lock should not be poisoned")
            .insert(delta_id.to_owned(), compressed);
    }
}

impl DeltaRepository for InMemoryDeltaRepository {
    async fn load_branch_chain(&self, branch_id: &str) -> Result<Vec<DeltaShot>> {
        Ok(self
            .branch_chains
            .read()
            .expect("lock should not be poisoned")
            .get(branch_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn load_ops(&self, deltashot_id: &str) -> Result<Vec<Op>> {
        self.ops
            .read()
            .expect("lock should not be poisoned")
            .get(deltashot_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("ops not found for {}", deltashot_id))
    }

    async fn load_compressed_ops(&self, deltashot_id: &str) -> Result<Vec<u8>> {
        self.compressed_ops
            .read()
            .expect("lock should not be poisoned")
            .get(deltashot_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("compressed ops not found for {}", deltashot_id))
    }
}

#[derive(Debug, Default)]
pub struct InMemoryStateRepository {
    states: RwLock<HashMap<String, Value>>,
    snapshots: RwLock<HashMap<String, (Value, usize)>>,
}

impl InMemoryStateRepository {
    pub fn set_branch_state(&self, branch_id: &str, state: Value) {
        self.states
            .write()
            .expect("lock should not be poisoned")
            .insert(branch_id.to_owned(), state);
    }

    pub fn set_snapshot(&self, branch_id: &str, state: Value, start_index: usize) {
        self.snapshots
            .write()
            .expect("lock should not be poisoned")
            .insert(branch_id.to_owned(), (state, start_index));
    }
}

impl StateRepository for InMemoryStateRepository {
    async fn get_branch_state(&self, branch_id: &str) -> Result<Value> {
        Ok(self
            .states
            .read()
            .expect("lock should not be poisoned")
            .get(branch_id)
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new())))
    }

    async fn load_nearest_snapshot(&self, branch_id: &str) -> Result<(Value, usize)> {
        Ok(self
            .snapshots
            .read()
            .expect("lock should not be poisoned")
            .get(branch_id)
            .cloned()
            .unwrap_or((Value::Object(serde_json::Map::new()), 0)))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use serde_json::json;

    use super::*;
    use deltashot::{canonical_serialize_ops, Metadata, OpType};

    fn compress(data: &[u8]) -> Vec<u8> {
        let mut compressed = Vec::new();
        let mut reader = brotli::CompressorReader::new(data, 4096, 5, 20);
        reader
            .read_to_end(&mut compressed)
            .expect("compression should succeed");
        compressed
    }

    #[tokio::test]
    async fn full_audit_passes_for_valid_chain_and_state() {
        let delta_repo = InMemoryDeltaRepository::default();
        let state_repo = InMemoryStateRepository::default();

        let ops1 = vec![Op {
            path: "root.a".to_owned(),
            op_type: OpType::Set,
            value: Some(json!(1)),
        }];
        let ops2 = vec![Op {
            path: "root.a".to_owned(),
            op_type: OpType::Replace,
            value: Some(json!(2)),
        }];

        let hash1 = compute_chain_hash(
            "",
            &canonical_serialize_ops(&ops1).expect("canonical serialize"),
        );
        let hash2 = compute_chain_hash(
            &hash1,
            &canonical_serialize_ops(&ops2).expect("canonical serialize"),
        );

        let ds1 = DeltaShot {
            id: "ds_1".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: "".to_owned(),
            hash: hash1.clone(),
            timestamp: 1,
            ops: ops1.clone(),
            artifacts: vec![],
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };
        let ds2 = DeltaShot {
            id: "ds_2".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: hash1,
            hash: hash2,
            timestamp: 2,
            ops: ops2.clone(),
            artifacts: vec![],
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };

        delta_repo.insert_branch_chain("br_main", vec![ds1.clone(), ds2.clone()]);
        delta_repo.insert_ops(
            &ds1.id,
            ops1.clone(),
            compress(&serde_json::to_vec(&ops1).expect("serialize")),
        );
        delta_repo.insert_ops(
            &ds2.id,
            ops2.clone(),
            compress(&serde_json::to_vec(&ops2).expect("serialize")),
        );

        state_repo.set_snapshot("br_main", json!({}), 0);
        state_repo.set_branch_state("br_main", json!({ "a": 2 }));

        let chain = verify_chain(&delta_repo, "br_main")
            .await
            .expect("verify should run");
        assert!(chain.valid);

        let replay = audit_replay(&delta_repo, &state_repo, "br_main")
            .await
            .expect("replay should run");
        assert!(replay.valid);
        assert!(replay.hash_match);
        assert!(replay.final_state_match);

        full_audit(&delta_repo, &state_repo, "br_main")
            .await
            .expect("full audit should pass");
    }

    #[tokio::test]
    async fn verify_chain_reports_hash_mismatch() {
        let delta_repo = InMemoryDeltaRepository::default();
        let state_repo = InMemoryStateRepository::default();

        let ops1 = vec![Op {
            path: "root.a".to_owned(),
            op_type: OpType::Set,
            value: Some(json!(1)),
        }];
        let ops2 = vec![Op {
            path: "root.a".to_owned(),
            op_type: OpType::Replace,
            value: Some(json!(2)),
        }];

        let hash1 = compute_chain_hash(
            "",
            &canonical_serialize_ops(&ops1).expect("canonical serialize"),
        );

        let ds1 = DeltaShot {
            id: "ds_1".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: "".to_owned(),
            hash: hash1.clone(),
            timestamp: 1,
            ops: ops1.clone(),
            artifacts: vec![],
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };
        let ds2 = DeltaShot {
            id: "ds_2".to_owned(),
            session_id: "sess_1".to_owned(),
            branch_id: "br_main".to_owned(),
            prev_hash: hash1,
            hash: "tampered".to_owned(),
            timestamp: 2,
            ops: ops2.clone(),
            artifacts: vec![],
            metadata: Metadata {
                event_type: "STATE_UPDATE".to_owned(),
                agent: None,
                workflow_step: None,
            },
        };

        delta_repo.insert_branch_chain("br_main", vec![ds1, ds2.clone()]);
        delta_repo.insert_ops(
            "ds_2",
            ops2.clone(),
            compress(&serde_json::to_vec(&ops2).expect("serialize")),
        );
        state_repo.set_snapshot("br_main", json!({}), 0);
        state_repo.set_branch_state("br_main", json!({ "a": 2 }));

        let result = verify_chain(&delta_repo, "br_main")
            .await
            .expect("verify should run");
        assert!(!result.valid);
        assert_eq!(result.failed_at.as_deref(), Some(ds2.id.as_str()));
    }
}
