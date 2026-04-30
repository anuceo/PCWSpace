/// WorkflowBolt — executes a single workflow step for the given `workflow_id`.
///
/// Receives `(workflow_id,)` tuples from the WorkflowSpout. Multiple instances
/// run in parallel across Storm workers so many workflow steps execute
/// concurrently without competing on a single Redis-polling loop.
use async_trait::async_trait;
use serde_json::Value;
use storm::{Bolt, Tuple};
use tracing::{error, info, instrument};
use workflows::{engine::WorkflowEngine, executor::StepExecutor};
use infra::redis_client::{WORKFLOW_STREAM, DEAD_LETTER_STREAM, get_multiplexed_connection};
use redis::AsyncCommands;

pub struct WorkflowBolt {
    executor: StepExecutor,
}

impl WorkflowBolt {
    pub fn new() -> Self {
        Self { executor: StepExecutor::new() }
    }
}

#[async_trait]
impl Bolt for WorkflowBolt {
    #[instrument(skip(self, tuple), fields(workflow_id))]
    async fn process(&mut self, tuple: Tuple) -> Result<Vec<(Option<String>, Vec<Value>)>, String> {
        let workflow_id = tuple.values
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing workflow_id in tuple".to_string())?
            .to_string();

        tracing::Span::current().record("workflow_id", &workflow_id);

        let mut conn = get_multiplexed_connection().await
            .map_err(|e| e.to_string())?;

        let mut state = WorkflowEngine::get_state(&workflow_id, &mut conn).await
            .map_err(|e| e.to_string())?;
        let def = WorkflowEngine::get_definition(&state.workflow_def_id, &mut conn).await
            .map_err(|e| e.to_string())?;

        match self.executor.execute_current_step(&state, &def).await {
            Ok(result) => {
                WorkflowEngine::advance(&mut state, result, &def, &mut conn).await
                    .map_err(|e| e.to_string())?;
                infra::metrics::global().increment(infra::metrics::names::WORKFLOWS_COMPLETED);
                info!(workflow_id, "Workflow step completed via Storm bolt");

                // If the workflow has more steps, re-enqueue for the next bolt pass
                if state.current_step.is_some() {
                    let _: Result<String, _> = conn
                        .xadd(WORKFLOW_STREAM, "*", &[("workflow_id", workflow_id.as_str())])
                        .await;
                }
                Ok(vec![])
            }
            Err(e) => {
                error!(workflow_id, error = %e, "Workflow step failed in Storm bolt");
                WorkflowEngine::fail(&mut state, &e.to_string(), true, &mut conn).await.ok();
                // Move to dead-letter
                let _: Result<String, _> = conn
                    .xadd(DEAD_LETTER_STREAM, "*",
                          &[("workflow_id", workflow_id.as_str()), ("error", e.to_string().as_str())])
                    .await;
                Err(e.to_string())
            }
        }
    }
}
