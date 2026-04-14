use anyhow::Result;
use serde_json::Value;

use crate::verifier::{decompress_ops, DeltaRepository};

#[derive(Debug, Default)]
pub struct ReplayEngine;

impl ReplayEngine {
    pub async fn replay_branch(
        &self,
        delta_repo: &impl DeltaRepository,
        branch_id: &str,
    ) -> Result<Value> {
        let deltas = delta_repo.load_branch_chain(branch_id).await?;
        let mut state = serde_json::json!({});

        for delta in deltas {
            let compressed = delta_repo.load_compressed_ops(&delta.id).await?;
            let decompressed = decompress_ops(&compressed)?;
            let ops: Vec<deltashot::Op> = serde_json::from_slice(&decompressed)?;
            deltashot::apply_ops(&mut state, &ops)?;
        }

        Ok(state)
    }
}
