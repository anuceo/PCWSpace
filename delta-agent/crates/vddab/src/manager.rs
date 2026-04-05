use std::path::PathBuf;

use crate::layout::SliceLayout;

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
}
