use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{WorkflowDefinition, WorkflowState, WorkflowStatus, StepStatus, now},
};
use infra::redis_client::{key_workflow_def, key_workflow_state};
use redis::AsyncCommands;
use tracing::{debug, instrument, warn};

pub struct WorkflowEngine;

impl WorkflowEngine {
    /// Start a workflow from a definition; persist state in Redis.
    #[instrument(skip(conn))]
    pub async fn start(
        def: &WorkflowDefinition,
        session_id: &str,
        initial_context: std::collections::HashMap<String, serde_json::Value>,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<WorkflowState> {
        // Persist definition
        let def_key = key_workflow_def(&def.workflow_def_id);
        let def_json = serde_json::to_string(def)
            .map_err(|e| PcwError::SerializationError(e.to_string()))?;
        let _: () = conn
            .set(&def_key, def_json)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        let mut state = WorkflowState::new(def, session_id);
        state.context = initial_context;

        Self::persist_state(&state, conn).await?;
        debug!(workflow_id = %state.workflow_id, "Workflow started");
        Ok(state)
    }

    pub async fn get_state(
        workflow_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<WorkflowState> {
        let key = key_workflow_state(workflow_id);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        let json = raw.ok_or_else(|| PcwError::WorkflowNotFound(workflow_id.to_string()))?;
        serde_json::from_str(&json).map_err(|e| PcwError::SerializationError(e.to_string()))
    }

    pub async fn get_definition(
        def_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<WorkflowDefinition> {
        let key = key_workflow_def(def_id);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        let json = raw.ok_or_else(|| PcwError::WorkflowDefNotFound(def_id.to_string()))?;
        serde_json::from_str(&json).map_err(|e| PcwError::SerializationError(e.to_string()))
    }

    /// Advance to the next step in the state machine.
    pub async fn advance(
        state: &mut WorkflowState,
        step_result: serde_json::Value,
        def: &WorkflowDefinition,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let current = match &state.current_step {
            Some(s) => s.clone(),
            None => {
                state.status = WorkflowStatus::Completed;
                state.completed_at = Some(now());
                Self::persist_state(state, conn).await?;
                return Ok(());
            }
        };

        // Store step result
        state.step_results.insert(current.clone(), step_result);
        state.step_status = StepStatus::Completed;
        state.retry_count = 0;

        // Find next step
        let next = def
            .state_transitions
            .get(&current)
            .and_then(|nexts| nexts.first())
            .cloned();

        if let Some(next_step) = next {
            // Validate transition exists in definition
            let valid = def.steps.iter().any(|s| s.name == next_step);
            if !valid {
                return Err(PcwError::InvalidTransition {
                    from: current,
                    to: next_step,
                });
            }
            state.current_step = Some(next_step);
            state.step_status = StepStatus::Pending;
        } else {
            state.current_step = None;
            state.status = WorkflowStatus::Completed;
            state.completed_at = Some(now());
        }

        state.updated_at = now();
        Self::persist_state(state, conn).await?;
        Ok(())
    }

    pub async fn fail(
        state: &mut WorkflowState,
        error: &str,
        dead_letter: bool,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        if dead_letter {
            state.step_status = StepStatus::DeadLettered;
            state.status = WorkflowStatus::Failed;
        } else {
            state.retry_count += 1;
            state.step_status = StepStatus::Failed;
        }
        state.error = Some(error.to_string());
        state.updated_at = now();
        Self::persist_state(state, conn).await?;
        warn!(workflow_id = %state.workflow_id, error, "Workflow step failed");
        Ok(())
    }

    async fn persist_state(
        state: &WorkflowState,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let key = key_workflow_state(&state.workflow_id);
        let json = serde_json::to_string(state)
            .map_err(|e| PcwError::SerializationError(e.to_string()))?;
        let _: () = conn
            .set(&key, json)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        Ok(())
    }
}
