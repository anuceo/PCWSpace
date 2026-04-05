use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub topic: String,
    pub payload: String,
}

#[derive(Debug, Clone)]
pub struct EventBus {
    sender: broadcast::Sender<StreamEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    pub fn publish(&self, event: StreamEvent) -> Result<usize, broadcast::error::SendError<StreamEvent>> {
        self.sender.send(event)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.sender.subscribe()
    }
}
