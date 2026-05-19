use crate::bus::StreamEvent;

pub fn format_websocket_message(event: &StreamEvent) -> String {
    format!("{}:{}", event.topic, event.payload)
}
