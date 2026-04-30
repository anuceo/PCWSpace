/// WorkflowSpout — reads pending workflow jobs from Redis and emits them into
/// the Storm topology as `(workflow_id,)` tuples on the "default" stream.
///
/// Replaces the blocking XREADGROUP poller in `scheduler.rs`. Storm manages
/// parallelism and retry; we only need to read + ack/nack.
use async_trait::async_trait;
use redis::AsyncCommands;
use serde_json::{json, Value};
use storm::Spout;
use tracing::{debug, error, warn};
use infra::redis_client::{WORKFLOW_STREAM, CONSUMER_GROUP};

pub struct WorkflowSpout {
    conn: Option<redis::aio::MultiplexedConnection>,
    /// Track pending ids so we can nack on fail
    pending: std::collections::HashMap<String, String>,
}

impl WorkflowSpout {
    pub fn new() -> Self {
        Self { conn: None, pending: Default::default() }
    }
}

#[async_trait]
impl Spout for WorkflowSpout {
    async fn open(&mut self, _conf: &Value, _context: &Value) {
        match infra::redis_client::get_multiplexed_connection().await {
            Ok(c) => { self.conn = Some(c); }
            Err(e) => error!("WorkflowSpout: Redis connect failed: {e}"),
        }
    }

    async fn next_tuple(&mut self) -> Option<(String, Option<String>, Vec<Value>)> {
        let conn = self.conn.as_mut()?;

        // Ensure consumer group exists
        let _: Result<(), _> = redis::cmd("XGROUP")
            .arg("CREATE").arg(WORKFLOW_STREAM).arg(CONSUMER_GROUP).arg("0").arg("MKSTREAM")
            .query_async::<()>(conn).await;

        let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP").arg(CONSUMER_GROUP).arg("pcw-storm-spout")
            .arg("COUNT").arg(1)
            .arg("BLOCK").arg(200)   // 200 ms — short block so Storm doesn't time out
            .arg("STREAMS").arg(WORKFLOW_STREAM).arg(">")
            .query_async(conn).await.ok()?;

        for key in &reply.keys {
            for entry in &key.ids {
                let workflow_id = get_field(&entry.map, "workflow_id");
                if workflow_id.is_empty() { continue; }

                // Use the Redis stream entry id as the Storm message id so we
                // can XACK it on ack() and XDEL+re-enqueue on fail().
                let storm_id = entry.id.clone();
                self.pending.insert(storm_id.clone(), workflow_id.clone());

                debug!(workflow_id, entry_id = %storm_id, "Spout emitting workflow job");
                return Some((storm_id, None, vec![json!(workflow_id)]));
            }
        }
        None
    }

    async fn ack(&mut self, id: String) {
        if let Some(conn) = self.conn.as_mut() {
            let _: Result<(), _> = conn.xack(WORKFLOW_STREAM, CONSUMER_GROUP, &[id.as_str()]).await;
            self.pending.remove(&id);
        }
    }

    async fn fail(&mut self, id: String) {
        // Re-enqueue so the workflow_id gets retried by another bolt instance
        if let Some(workflow_id) = self.pending.remove(&id) {
            if let Some(conn) = self.conn.as_mut() {
                warn!(workflow_id, "Re-enqueuing failed workflow job");
                let _: Result<String, _> = conn
                    .xadd(WORKFLOW_STREAM, "*", &[("workflow_id", workflow_id.as_str())])
                    .await;
                // ACK the original so it leaves the PEL
                let _: Result<(), _> = conn.xack(WORKFLOW_STREAM, CONSUMER_GROUP, &[id.as_str()]).await;
            }
        }
    }
}

fn get_field(map: &std::collections::HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    }
}
