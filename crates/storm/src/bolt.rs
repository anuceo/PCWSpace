/// Storm Multilang Bolt runner.
///
/// Implement the `Bolt` trait, then call `run_bolt()` from your binary's
/// `main`. Storm manages lifecycle via stdin/stdout per the Multilang protocol.
use std::io::{self, BufReader};
use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, error, info, warn};
use crate::protocol::{self, ComponentMsg, TupleMsg};

/// A tuple received from Storm, ready to be processed.
pub struct Tuple {
    pub id:     String,
    pub comp:   String,
    pub stream: String,
    pub values: Vec<Value>,
}

/// Implement this trait for each Storm bolt component.
#[async_trait]
pub trait Bolt: Send + Sync {
    /// Called once when the bolt receives its Storm configuration.
    async fn prepare(&mut self, _conf: &Value, _context: &Value) {}

    /// Process one tuple. Return `Ok(emits)` to ack; `Err(_)` to fail.
    /// `emits` is a list of (stream, values) tuples to emit downstream.
    async fn process(&mut self, tuple: Tuple) -> Result<Vec<(Option<String>, Vec<Value>)>, String>;
}

/// Run the bolt event loop — reads Storm messages from stdin, dispatches to
/// the `Bolt` implementation, writes acks/emits/fails to stdout.
pub async fn run_bolt(mut bolt: impl Bolt) {
    let stdin  = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = stdout.lock();

    // Handshake
    match protocol::read_msg(&mut reader) {
        Ok(raw) => {
            match serde_json::from_str::<protocol::HandshakeMsg>(&raw) {
                Ok(hs) => {
                    if let Err(e) = protocol::ack_handshake(&mut writer, &hs.pid_dir) {
                        error!("Failed to ack handshake: {e}");
                        return;
                    }
                    bolt.prepare(&hs.conf, &hs.context).await;
                    info!("Bolt ready (pid={})", std::process::id());
                }
                Err(e) => {
                    error!("Bad handshake: {e} raw={raw}");
                    return;
                }
            }
        }
        Err(e) => { error!("Handshake read error: {e}"); return; }
    }

    // Main processing loop
    loop {
        let raw = match protocol::read_msg(&mut reader) {
            Ok(r) => r,
            Err(e) => {
                warn!("Bolt read error: {e}");
                break;
            }
        };

        // Heartbeat — Storm sends id="-1" on the __heartbeat__ stream
        if raw.contains("__heartbeat__") {
            let _ = protocol::send_msg(&mut writer, &ComponentMsg::Sync);
            continue;
        }

        let tuple_msg: TupleMsg = match serde_json::from_str(&raw) {
            Ok(t) => t,
            Err(e) => { warn!("Bad tuple: {e}"); continue; }
        };

        let msg_id  = tuple_msg.id.clone();
        let anchors = vec![msg_id.clone()];

        let tuple = Tuple {
            id:     tuple_msg.id,
            comp:   tuple_msg.comp,
            stream: tuple_msg.stream,
            values: tuple_msg.tuple,
        };

        match bolt.process(tuple).await {
            Ok(emits) => {
                for (stream, values) in emits {
                    let emit = ComponentMsg::Emit {
                        anchors: anchors.clone(),
                        stream,
                        tuple: values,
                        id: None,
                    };
                    if let Err(e) = protocol::send_msg(&mut writer, &emit) {
                        error!("Emit write error: {e}");
                    }
                }
                debug!(id = %msg_id, "Acking tuple");
                let _ = protocol::send_msg(&mut writer, &ComponentMsg::Ack { id: msg_id });
            }
            Err(reason) => {
                warn!(id = %msg_id, reason, "Failing tuple");
                let _ = protocol::send_msg(&mut writer, &ComponentMsg::Fail { id: msg_id });
            }
        }
    }
}
