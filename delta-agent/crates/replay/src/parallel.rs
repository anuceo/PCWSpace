use std::sync::Arc;

use anyhow::Result;

use crate::verifier::{full_audit, DeltaRepository, StateRepository};

pub async fn full_audit_parallel(
    delta_repo: Arc<impl DeltaRepository + Send + Sync + 'static>,
    state_repo: Arc<impl StateRepository + Send + Sync + 'static>,
    branches: Vec<String>,
) -> Vec<(String, Result<()>)> {
    let tasks = branches
        .into_iter()
        .map(|branch_id| {
            let delta_repo = Arc::clone(&delta_repo);
            let state_repo = Arc::clone(&state_repo);
            tokio::spawn(async move {
                let result = full_audit(delta_repo.as_ref(), state_repo.as_ref(), &branch_id).await;
                (branch_id, result)
            })
        })
        .collect::<Vec<_>>();

    let mut outcomes = Vec::new();
    for task in tasks {
        match task.await {
            Ok(result) => outcomes.push(result),
            Err(join_err) => outcomes.push((
                "unknown".to_owned(),
                Err(anyhow::anyhow!("parallel audit join failed: {join_err}")),
            )),
        }
    }
    outcomes
}
