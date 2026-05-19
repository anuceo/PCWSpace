use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SliceLayout {
    pub branch_id: String,
    pub root: PathBuf,
}

impl SliceLayout {
    pub fn deltashot_dir(&self) -> PathBuf {
        self.root.join(&self.branch_id).join("deltashots")
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.root.join(&self.branch_id).join("snapshots")
    }

    pub fn artifact_dir(&self) -> PathBuf {
        self.root.join(&self.branch_id).join("artifacts")
    }
}
