use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub id: String,
    pub branch_id: String,
    pub base_snapshot: Option<String>,
    pub delta_hash: String,
}
