use tracing::debug;

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
        let event = StreamEvent {
            topic: topic.into(),
            payload: payload.into(),
        };
        if let Err(_) = self.bus.publish(event) {
            debug!("stream event dropped: no active subscribers");
        }
    }
}
