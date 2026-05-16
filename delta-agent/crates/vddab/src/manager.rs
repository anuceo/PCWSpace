use std::path::PathBuf;

use crate::layout::SliceLayout;
use crate::{deltashot_store::DeltaShotStore, snapshot_store::SnapshotStore};

#[derive(Debug, Clone)]
pub struct VddabManager {
    root: PathBuf,
}

impl VddabManager {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn layout_for_branch(&self, branch_id: impl Into<String>) -> SliceLayout {
        SliceLayout {
            branch_id: branch_id.into(),
            root: self.root.clone(),
        }
    }

    pub fn deltashot_store(&self) -> DeltaShotStore {
        DeltaShotStore::new(self.root.clone())
    }

    pub fn snapshot_store(&self) -> SnapshotStore {
        SnapshotStore::new(self.root.clone())
    }
}
