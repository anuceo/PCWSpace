use pcw_core::errors::PcwResult;
use deltashots::store::{count_shots, get_all_shots};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineNode {
    pub session_id: String,
    pub shot_count: u64,
    pub forked_from: Option<String>,
    pub fork_at_sequence: Option<u64>,
}

/// Return timeline nodes for a session (including its fork ancestry).
pub async fn get_timeline(
    session_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Vec<TimelineNode>> {
    let count = count_shots(session_id, conn).await?;

    let shots = get_all_shots(session_id, conn).await?;
    let fork_meta = shots.iter().find(|s| s.action == "BRANCH_FORK").and_then(|s| {
        let from = s.metadata.get("source_session_id").and_then(|v| v.as_str()).map(str::to_string);
        let at = s.metadata.get("fork_at_sequence").and_then(|v| v.as_u64());
        from.zip(at)
    });

    let node = TimelineNode {
        session_id: session_id.to_string(),
        shot_count: count,
        forked_from: fork_meta.as_ref().map(|(f, _)| f.clone()),
        fork_at_sequence: fork_meta.as_ref().map(|(_, s)| *s),
    };

    Ok(vec![node])
}

/// List rollback points (just sequence numbers + actions) for UI display.
pub async fn list_rollback_points(
    session_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<Vec<serde_json::Value>> {
    deltashots::replay::list_rollback_points(session_id, conn).await
}
