use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;

#[derive(Debug, Clone)]
pub struct SnapshotStore {
    pub root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub branch_id: String,
    pub start_index: usize,
    pub state: Value,
}

impl SnapshotStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn snapshot_path(&self, branch_id: &str, start_index: usize) -> PathBuf {
        self.root
            .join(branch_id)
            .join("snapshots")
            .join(format!("{start_index}.json"))
    }

    pub async fn write_snapshot(
        &self,
        branch_id: &str,
        start_index: usize,
        state: &Value,
    ) -> std::io::Result<PathBuf> {
        let path = self.snapshot_path(branch_id, start_index);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let payload = serde_json::to_vec(&SnapshotRecord {
            branch_id: branch_id.to_owned(),
            start_index,
            state: state.clone(),
        })
        .map_err(json_err)?;
        fs::write(&path, payload).await?;
        Ok(path)
    }

    pub async fn read_nearest_snapshot(
        &self,
        branch_id: &str,
    ) -> std::io::Result<Option<(Value, usize)>> {
        let dir = self.root.join(branch_id).join("snapshots");
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            return Ok(None);
        }

        let mut entries = fs::read_dir(&dir).await?;
        let mut best: Option<(usize, PathBuf)> = None;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(index) = stem.parse::<usize>() else {
                continue;
            };
            match best {
                Some((best_idx, _)) if best_idx >= index => {}
                _ => best = Some((index, path)),
            }
        }

        let Some((index, path)) = best else {
            return Ok(None);
        };
        let payload = fs::read(path).await?;
        let snapshot: SnapshotRecord = serde_json::from_slice(&payload).map_err(json_err)?;
        Ok(Some((snapshot.state, index)))
    }
}

fn json_err(err: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}
