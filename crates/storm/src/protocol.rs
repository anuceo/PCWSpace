/// Storm Multilang Protocol — JSON messages delimited by a literal "end\n" line.
///
/// Storm communicates with non-JVM components (spouts/bolts) over stdin/stdout
/// using this protocol. Each logical message is a sequence of JSON lines
/// terminated by a bare "end" line.
///
/// Reference: https://storm.apache.org/releases/current/Multilang-protocol.html
use std::io::{self, BufRead, Write};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Message types sent by Storm to a bolt ────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct HandshakeMsg {
    pub conf:    Value,
    pub context: Value,
    #[serde(rename = "pidDir")]
    pub pid_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct TupleMsg {
    pub id:     String,
    pub comp:   String,
    pub stream: String,
    pub task:   i64,
    pub tuple:  Vec<Value>,
}

// ── Messages sent by Storm to a spout ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SpoutCommand {
    pub command: String,
    pub id:      Option<String>,
}

// ── Messages the component sends back ────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(tag = "command", rename_all = "lowercase")]
pub enum ComponentMsg {
    Emit {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        anchors: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream:  Option<String>,
        tuple:   Vec<Value>,
        /// Only used by spouts — unique message id for ack/fail tracking.
        #[serde(skip_serializing_if = "Option::is_none")]
        id:      Option<String>,
    },
    Ack { id: String },
    Fail { id: String },
    Log { msg: String },
    Sync,
}

// ── Low-level framing ─────────────────────────────────────────────────────────

/// Read one logical multilang message from stdin.
/// Returns the concatenated JSON lines up to (not including) the "end" line.
pub fn read_msg(reader: &mut impl BufRead) -> io::Result<String> {
    let mut parts = Vec::new();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "storm closed pipe"));
        }
        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        if trimmed == "end" {
            break;
        }
        parts.push(trimmed.to_string());
    }
    Ok(parts.join(""))
}

/// Send one logical multilang message to stdout, terminated by "end\n".
pub fn send_msg(writer: &mut impl Write, msg: &impl Serialize) -> io::Result<()> {
    let json = serde_json::to_string(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writeln!(writer, "{json}")?;
    writeln!(writer, "end")?;
    writer.flush()
}

/// Acknowledge the handshake by writing the process PID and creating the pid file.
pub fn ack_handshake(writer: &mut impl Write, pid_dir: &str) -> io::Result<()> {
    let pid = std::process::id();
    // Storm expects a file named after the PID in pidDir
    let pid_file = format!("{pid_dir}/{pid}");
    let _ = std::fs::write(&pid_file, "");
    let pid_msg = serde_json::json!({ "pid": pid });
    writeln!(writer, "{pid_msg}")?;
    writeln!(writer, "end")?;
    writer.flush()
}
