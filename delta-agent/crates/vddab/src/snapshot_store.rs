use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SnapshotStore {
    pub root: PathBuf,
}
