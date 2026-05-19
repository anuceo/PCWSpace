use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaShot {
    pub id: Uuid,
    pub branch_id: String,
    pub operations: Vec<String>,
    pub created_at_ms: u64,
}
