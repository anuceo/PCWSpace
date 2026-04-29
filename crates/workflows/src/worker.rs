use pcw_core::errors::{PcwError, PcwResult};
use infra::redis_client::{WORKFLOW_STREAM, DEAD_LETTER_STREAM, CONSUMER_GROUP};
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument};

use crate::{engine::WorkflowEngine, executor::StepExecutor};

/// One-shot: read a pending workflow job from the stream and execute it.
#[instrument(skip(conn))]
pub async fn process_next(conn: &mut redis::aio::MultiplexedConnection) -> PcwResult<bool> {
    // Ensure consumer group exists (ignore error if already exists)
    let _: Result<(), _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(WORKFLOW_STREAM)
        .arg(CONSUMER_GROUP)
        .arg("0")
        .arg("MKSTREAM")
        .query_async::<()>(conn)
        .await;

    let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(CONSUMER_GROUP)
        .arg("pcw-worker-1")
        .arg("COUNT")
        .arg(1)
        .arg("BLOCK")
        .arg(2000)
        .arg("STREAMS")
        .arg(WORKFLOW_STREAM)
        .arg(">")
        .query_async(conn)
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;

    for stream_key in &reply.keys {
        for entry in &stream_key.ids {
            let workflow_id = get_field(&entry.map, "workflow_id");
            if workflow_id.is_empty() {
                continue;
            }
            debug!(workflow_id, "Processing workflow job");

            let executor = StepExecutor::new();
            let mut state = WorkflowEngine::get_state(&workflow_id, conn).await?;
            let def = WorkflowEngine::get_definition(&state.workflow_def_id, conn).await?;

            match executor.execute_current_step(&state, &def).await {
                Ok(result) => {
                    WorkflowEngine::advance(&mut state, result, &def, conn).await?;
                    // ACK
                    let _: () = conn
                        .xack(WORKFLOW_STREAM, CONSUMER_GROUP, &[entry.id.as_str()])
                        .await
                        .map_err(|e| PcwError::RedisError(e.to_string()))?;
                    info!(workflow_id, "Workflow step completed");
                }
                Err(e) => {
                    error!(workflow_id, error = %e, "Workflow step failed");
                    WorkflowEngine::fail(&mut state, &e.to_string(), true, conn).await?;
                    // Move to dead-letter stream
                    let _: String = conn
                        .xadd(
                            DEAD_LETTER_STREAM,
                            "*",
                            &[("workflow_id", workflow_id.as_str()), ("error", e.to_string().as_str())],
                        )
                        .await
                        .map_err(|e| PcwError::RedisError(e.to_string()))?;
                    let _: () = conn
                        .xack(WORKFLOW_STREAM, CONSUMER_GROUP, &[entry.id.as_str()])
                        .await
                        .map_err(|e| PcwError::RedisError(e.to_string()))?;
                }
            }
            return Ok(true);
        }
    }
    Ok(false)
}

/// Enqueue a workflow step for processing.
pub async fn enqueue(
    workflow_id: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> PcwResult<String> {
    let entry_id: String = conn
        .xadd(WORKFLOW_STREAM, "*", &[("workflow_id", workflow_id)])
        .await
        .map_err(|e| PcwError::RedisError(e.to_string()))?;
    Ok(entry_id)
}

fn get_field(map: &std::collections::HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => String::new(),
    }
}
