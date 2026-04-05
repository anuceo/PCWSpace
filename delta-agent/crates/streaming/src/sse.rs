use crate::bus::StreamEvent;

pub fn format_sse(event: &StreamEvent) -> String {
    format!("event: {}\\ndata: {}\\n\\n", event.topic, event.payload)
}
