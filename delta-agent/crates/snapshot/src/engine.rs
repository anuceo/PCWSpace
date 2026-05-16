use crate::delta::SnapshotDelta;
use crate::rebuild::rebuild_snapshot;

#[derive(Default)]
pub struct SnapshotEngine;

impl SnapshotEngine {
    pub fn rebuild(&self, base: &str, delta: &SnapshotDelta) -> String {
        rebuild_snapshot(base, delta)
    }
}
