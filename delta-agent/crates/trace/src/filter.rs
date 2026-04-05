use crate::timeline::TraceEvent;

pub fn by_branch<'a>(events: &'a [TraceEvent], branch_id: &str) -> Vec<&'a TraceEvent> {
    events
        .iter()
        .filter(|event| event.branch_id == branch_id)
        .collect::<Vec<_>>()
}
