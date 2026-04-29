use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{WorkflowDefinition, WorkflowState, WorkflowStep},
};
use agents::router::AgentRouter;
use tracing::{debug, instrument, warn};

pub struct StepExecutor {
    router: AgentRouter,
}

impl StepExecutor {
    pub fn new() -> Self {
        Self { router: AgentRouter::new() }
    }

    /// Execute a single workflow step with retry logic.
    #[instrument(skip(self, state, def), fields(step = ?state.current_step))]
    pub async fn execute_current_step(
        &self,
        state: &WorkflowState,
        def: &WorkflowDefinition,
    ) -> PcwResult<serde_json::Value> {
        let step_name = state
            .current_step
            .as_deref()
            .ok_or_else(|| PcwError::InvalidInput("No current step".into()))?;

        let step = def
            .steps
            .iter()
            .find(|s| s.name == step_name)
            .ok_or_else(|| PcwError::InvalidInput(format!("Step '{step_name}' not found in definition")))?;

        let mut last_err = String::new();
        for attempt in 0..=step.max_retries {
            if attempt > 0 {
                warn!(step = step_name, attempt, "Retrying workflow step");
                let delay = std::time::Duration::from_millis(500 * 2u64.pow(attempt - 1));
                tokio::time::sleep(delay).await;
            }
            match self.run_step(step, state).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_err = e.to_string();
                    debug!(step = step_name, attempt, error = %e, "Step attempt failed");
                }
            }
        }

        Err(PcwError::AgentError {
            agent: "executor".into(),
            reason: format!("Step '{step_name}' failed after {} attempts: {last_err}", step.max_retries + 1),
        })
    }

    async fn run_step(
        &self,
        step: &WorkflowStep,
        state: &WorkflowState,
    ) -> PcwResult<serde_json::Value> {
        match step.handler.as_str() {
            "finalize" | "save" => {
                // Non-agent terminal steps — just return context summary
                return Ok(serde_json::json!({"status": "done", "context": state.step_results}));
            }
            _ => {}
        }

        let agent_type = step
            .agent_type
            .clone()
            .unwrap_or(pcw_core::models::AgentType::Claude);

        let prompt = step.prompt.as_deref().unwrap_or("Complete this workflow step.");

        // Inject previous step results as context
        let context_str = serde_json::to_string(&state.step_results).unwrap_or_default();
        let system = format!(
            "You are a workflow executor. Previous step results:\n{context_str}"
        );

        let messages = vec![pcw_core::models::Message::user(prompt)];

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(step.timeout_secs),
            self.router.call(agent_type, &messages, Some(&system)),
        )
        .await
        .map_err(|_| PcwError::AgentError {
            agent: step.handler.clone(),
            reason: format!("Step '{}' timed out after {}s", step.name, step.timeout_secs),
        })??;

        Ok(serde_json::json!({
            "response": result.response,
            "tokens_used": result.tokens_used,
            "model": result.model,
        }))
    }
}

impl Default for StepExecutor {
    fn default() -> Self {
        Self::new()
    }
}
