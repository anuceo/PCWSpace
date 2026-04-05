use crate::parallel::apply_parallel;

#[derive(Debug, Default)]
pub struct ReplayEngine;

impl ReplayEngine {
    pub fn replay(&self, events: &[String]) -> Vec<String> {
        apply_parallel(events)
    }
}
