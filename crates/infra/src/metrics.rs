use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

// ── SimpleMetrics ──────────────────────────────────────────────────────────

/// A lightweight, globally-accessible counter registry backed by a
/// `Mutex<HashMap<String, u64>>`.
///
/// All operations are O(n) in the number of distinct counter names, which is
/// fine for a small, fixed set of application-level metrics.
pub struct SimpleMetrics(Mutex<HashMap<String, u64>>);

static SIMPLE: OnceLock<SimpleMetrics> = OnceLock::new();

/// Returns a reference to the process-global `SimpleMetrics` instance.
pub fn global() -> &'static SimpleMetrics {
    SIMPLE.get_or_init(|| SimpleMetrics(Mutex::new(HashMap::new())))
}

impl SimpleMetrics {
    /// Increment the named counter by 1, creating it at 0 if it does not exist.
    pub fn increment(&self, name: &str) {
        let mut m = self.0.lock().expect("metrics lock poisoned");
        *m.entry(name.to_string()).or_insert(0) += 1;
    }

    /// Return the current value of the named counter (0 if it has never been incremented).
    pub fn get(&self, name: &str) -> u64 {
        self.0
            .lock()
            .expect("metrics lock poisoned")
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// Return a point-in-time snapshot of all counters.
    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.0.lock().expect("metrics lock poisoned").clone()
    }
}

// ── Well-known counter names ───────────────────────────────────────────────

pub mod names {
    pub const SESSIONS_CREATED: &str = "sessions.created";
    pub const SESSIONS_CLOSED: &str = "sessions.closed";
    pub const DELTASHOTS_APPENDED: &str = "deltashots.appended";
    pub const AGENT_CALLS: &str = "agent.calls";
    pub const AGENT_ERRORS: &str = "agent.errors";
    pub const ARTIFACTS_CREATED: &str = "artifacts.created";
    pub const WORKFLOWS_STARTED: &str = "workflows.started";
    pub const WORKFLOWS_COMPLETED: &str = "workflows.completed";
    pub const NOTION_SYNCS: &str = "notion.syncs";
    pub const REPLAY_CALLS: &str = "replay.calls";
}
