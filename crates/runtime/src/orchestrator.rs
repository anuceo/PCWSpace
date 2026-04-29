use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{AgentResult, AgentType, Message, Session, SessionStatus, now},
};
use deltashots::engine::{append_deltashot, AppendParams};
use infra::redis_client::key_session_meta;
use agents::router::AgentRouter;
use intelligence::analyzer::analyze_task;
use redis::AsyncCommands;
use tracing::{debug, info, instrument};
use std::collections::HashMap;

pub struct Orchestrator {
    router: AgentRouter,
}

impl Orchestrator {
    pub fn new() -> Self {
        Self { router: AgentRouter::new() }
    }

    /// Load session state from Redis.
    pub async fn load_session(
        session_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<Session> {
        let key = key_session_meta(session_id);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        let json = raw.ok_or_else(|| PcwError::SessionNotFound(session_id.to_string()))?;
        serde_json::from_str(&json).map_err(|e| PcwError::SerializationError(e.to_string()))
    }

    /// Persist session state to Redis.
    pub async fn save_session(
        session: &Session,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let key = key_session_meta(&session.session_id);
        let json = serde_json::to_string(session)
            .map_err(|e| PcwError::SerializationError(e.to_string()))?;
        let _: () = conn
            .set(&key, json)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        Ok(())
    }

    /// Full agent call lifecycle: load → analyze → call → diff → deltashot → save.
    #[instrument(skip(self, conn), fields(session_id, agent_override = ?agent_override))]
    pub async fn call_agent(
        &self,
        session_id: &str,
        user_message: &str,
        agent_override: Option<AgentType>,
        system_prompt: Option<&str>,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<AgentResult> {
        // 1. Load state
        let mut session = Self::load_session(session_id, conn).await?;
        let before = session.to_state_object();

        // 2. Analyze & select agent
        let analysis = analyze_task(user_message);
        let agent_type = agent_override.unwrap_or(analysis.suggested_agent.clone());

        // 3. Add user message
        session.messages.push(Message::user(user_message));

        // 4. Call agent
        debug!(agent = %agent_type, "Calling agent");
        let mut result = self.router.call(agent_type.clone(), &session.messages, system_prompt).await?;

        // 5. Add assistant message to session
        session.messages.push(Message::assistant(result.response.clone(), agent_type.clone()));

        let after = session.to_state_object();

        // 6. Compute diff + DeltaShot
        let shot = append_deltashot(
            AppendParams {
                session_id,
                before,
                after,
                action: "AGENT_RESPONSE",
                agent_type: Some(agent_type),
                message_index: Some(session.messages.len() as u64 - 1),
                artifact_changes: vec![],
                metadata: HashMap::new(),
            },
            conn,
        )
        .await?;

        result.shot_id = Some(shot.deltashot_id);

        // 7. Persist updated session
        Self::save_session(&session, conn).await?;

        info!(session_id, tokens = result.tokens_used, "Agent call complete");
        Ok(result)
    }

    /// Create a new session and persist it.
    pub async fn create_session(
        workspace_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<Session> {
        let session = Session::new(workspace_id);
        Self::save_session(&session, conn).await?;
        info!(session_id = %session.session_id, "Session created");
        Ok(session)
    }

    /// Close a session.
    pub async fn close_session(
        session_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let mut session = Self::load_session(session_id, conn).await?;
        session.status = SessionStatus::Closed;
        session.closed_at = Some(now());
        Self::save_session(&session, conn).await
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}
