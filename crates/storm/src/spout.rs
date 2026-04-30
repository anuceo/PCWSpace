/// Storm Multilang Spout runner.
///
/// Implement the `Spout` trait, then call `run_spout()` from your binary's
/// `main`. Storm drives the `next_tuple` / `ack` / `fail` cycle via stdin.
use std::io::{self, BufReader};
use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, error, info, warn};
use crate::protocol::{self, ComponentMsg, SpoutCommand};

/// Implement this trait for each Storm spout component.
#[async_trait]
pub trait Spout: Send + Sync {
    /// Called once on startup with the Storm configuration.
    async fn open(&mut self, _conf: &Value, _context: &Value) {}

    /// Storm calls this when it is ready for the next tuple.
    /// Return `None` if there is nothing to emit right now.
    /// Return `Some((id, stream, values))` to emit a message.
    async fn next_tuple(&mut self) -> Option<(String, Option<String>, Vec<Value>)>;

    /// Storm successfully processed the tuple with this id.
    async fn ack(&mut self, _id: String) {}

    /// Storm failed the tuple with this id (should be re-emitted or discarded).
    async fn fail(&mut self, _id: String) {}
}

/// Run the spout event loop — driven by Storm commands on stdin.
pub async fn run_spout(mut spout: impl Spout) {
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
                        error!("Spout handshake ack failed: {e}");
                        return;
                    }
                    spout.open(&hs.conf, &hs.context).await;
                    info!("Spout ready (pid={})", std::process::id());
                }
                Err(e) => { error!("Bad spout handshake: {e}"); return; }
            }
        }
        Err(e) => { error!("Spout handshake read error: {e}"); return; }
    }

    loop {
        let raw = match protocol::read_msg(&mut reader) {
            Ok(r) => r,
            Err(e) => { warn!("Spout read error: {e}"); break; }
        };

        let cmd: SpoutCommand = match serde_json::from_str(&raw) {
            Ok(c) => c,
            Err(e) => { warn!("Bad spout command: {e}"); continue; }
        };

        match cmd.command.as_str() {
            "next" => {
                match spout.next_tuple().await {
                    Some((id, stream, tuple)) => {
                        debug!(id = %id, "Spout emitting tuple");
                        let emit = ComponentMsg::Emit {
                            anchors: vec![],
                            stream,
                            tuple,
                            id: Some(id),
                        };
                        let _ = protocol::send_msg(&mut writer, &emit);
                    }
                    None => {
                        // Nothing to emit — send sync so Storm doesn't starve
                        let _ = protocol::send_msg(&mut writer, &ComponentMsg::Sync);
                    }
                }
            }
            "ack"  => { spout.ack(cmd.id.unwrap_or_default()).await; }
            "fail" => { spout.fail(cmd.id.unwrap_or_default()).await; }
            other  => { warn!("Unknown spout command: {other}"); }
        }
    }
}
