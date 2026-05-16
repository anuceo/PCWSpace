#[derive(Debug, Clone)]
pub struct SnapshotDelta {
    pub before_id: String,
    pub after_id: String,
    pub changes: Vec<String>,
}
