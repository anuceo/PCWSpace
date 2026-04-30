/// DeltaShotBolt — buffers deltashot append requests and flushes them to Redis
/// in batches, reducing per-request write pressure.
///
/// Receives `(session_id, before_json, after_json, action, agent_type)` tuples.
/// Accumulates up to `BATCH_SIZE` items or `FLUSH_MS` milliseconds, whichever
/// comes first, then writes them all to Redis in a single pipeline pass.
use async_trait::async_trait;
use serde_json::Value;
use storm::{Bolt, Tuple};
use tracing::{debug, error, info};
use std::time::Instant;
use deltashots::engine::{append_deltashot, AppendParams};
use pcw_core::models::AgentType;
use std::collections::HashMap;

const BATCH_SIZE: usize = 20;
const FLUSH_MS:   u128  = 250;

struct PendingShot {
    session_id:   String,
    before:       Value,
    after:        Value,
    action:       String,
    agent_type:   Option<AgentType>,
    message_index: Option<u64>,
}

pub struct DeltaShotBolt {
    buffer:      Vec<PendingShot>,
    last_flush:  Instant,
}

impl DeltaShotBolt {
    pub fn new() -> Self {
        Self {
            buffer:     Vec::with_capacity(BATCH_SIZE),
            last_flush: Instant::now(),
        }
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() { return; }

        let shots = std::mem::replace(&mut self.buffer, Vec::with_capacity(BATCH_SIZE));
        let count = shots.len();
        self.last_flush = Instant::now();

        match infra::redis_client::get_multiplexed_connection().await {
            Err(e) => error!("DeltaShotBolt: Redis connect failed: {e}"),
            Ok(mut conn) => {
                for shot in shots {
                    let params = AppendParams {
                        session_id:      &shot.session_id,
                        before:          shot.before,
                        after:           shot.after,
                        action:          &shot.action,
                        agent_type:      shot.agent_type,
                        message_index:   shot.message_index,
                        artifact_changes: vec![],
                        metadata:        HashMap::new(),
                    };
                    match append_deltashot(params, &mut conn).await {
                        Ok(_) => {
                            infra::metrics::global().increment(infra::metrics::names::DELTASHOTS_APPENDED);
                            debug!(session_id = %shot.session_id, "DeltaShot flushed via bolt");
                        }
                        Err(e) => error!(session_id = %shot.session_id, "DeltaShot flush error: {e}"),
                    }
                }
                info!(count, "DeltaShotBolt batch flushed");
            }
        }
    }
}

#[async_trait]
impl Bolt for DeltaShotBolt {
    async fn process(&mut self, tuple: Tuple) -> Result<Vec<(Option<String>, Vec<Value>)>, String> {
        let vals = &tuple.values;
        let session_id = vals.get(0).and_then(|v| v.as_str())
            .ok_or("missing session_id")?.to_string();
        let before = vals.get(1).cloned().unwrap_or(Value::Object(Default::default()));
        let after  = vals.get(2).cloned().unwrap_or(Value::Object(Default::default()));
        let action = vals.get(3).and_then(|v| v.as_str()).unwrap_or("AGENT_RESPONSE").to_string();
        let agent_type = vals.get(4).and_then(|v| v.as_str()).and_then(|s| match s {
            "Claude"   => Some(AgentType::Claude),
            "DeepSeek" => Some(AgentType::DeepSeek),
            _          => None,
        });
        let message_index = vals.get(5).and_then(|v| v.as_u64());

        self.buffer.push(PendingShot { session_id, before, after, action, agent_type, message_index });

        let should_flush = self.buffer.len() >= BATCH_SIZE
            || self.last_flush.elapsed().as_millis() >= FLUSH_MS;

        if should_flush {
            self.flush().await;
        }

        Ok(vec![])
    }
}
