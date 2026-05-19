use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    pub id: String,
    pub name: String,
    pub priority: i32,
}
