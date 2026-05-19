use crate::delta::SnapshotDelta;

pub fn rebuild_snapshot(base: &str, delta: &SnapshotDelta) -> String {
    if delta.changes.is_empty() {
        return base.to_owned();
    }

    format!("{base}\n{}", delta.changes.join("\n"))
}
