use crate::bus::{EventBus, StreamEvent};

#[derive(Debug, Clone)]
pub struct Publisher {
    bus: EventBus,
}

impl Publisher {
    pub fn new(bus: EventBus) -> Self {
        Self { bus }
    }

    pub fn publish(&self, topic: impl Into<String>, payload: impl Into<String>) {
        let _ = self.bus.publish(StreamEvent {
            topic: topic.into(),
            payload: payload.into(),
        });
    }
}
