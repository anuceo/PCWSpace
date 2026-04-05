use crate::timeline::TraceEvent;

#[derive(Debug, Default)]
pub struct TraceViewer {
    events: Vec<TraceEvent>,
}

impl TraceViewer {
    pub fn push(&mut self, event: TraceEvent) {
        self.events.push(event);
    }

    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }
}
