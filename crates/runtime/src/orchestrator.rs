use pcw_core::{
    errors::{PcwError, PcwResult},
    models::{AgentResult, AgentType, Message, Session, SessionStatus, now},
};
use deltashots::engine::{append_deltashot, AppendParams};
use infra::{metrics, redis_client::key_session_meta};
use agents::router::AgentRouter;
use intelligence::analyzer::analyze_task;
use redis::AsyncCommands;
use tracing::{debug, info, instrument, warn};
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

    /// Persist session state to Redis with a sliding TTL.
    /// Every write resets the window to `SESSION_KEY_TTL_SECS` so active
    /// sessions never expire mid-conversation. Closed sessions get a longer
    /// fixed TTL applied by `close_session` immediately after this call.
    pub async fn save_session(
        session: &Session,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let key = key_session_meta(&session.session_id);
        let json = serde_json::to_string(session)
            .map_err(|e| PcwError::SerializationError(e.to_string()))?;
        let ttl = pcw_core::config::get_settings().session_key_ttl_secs;
        let _: () = conn
            .set_ex(&key, json, ttl)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;
        Ok(())
    }

    /// Full agent call lifecycle: load → analyze → route by quality → call → diff → deltashot → save.
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

        if session.status == SessionStatus::Closed {
            return Err(PcwError::InvalidInput(format!(
                "session {session_id} is closed and cannot accept new messages"
            )));
        }

        let before = session.to_state_object();

        // 2. Analyze & select agent — quality threshold may override category routing
        let analysis = analyze_task(user_message);
        let agent_type = agent_override.unwrap_or_else(|| {
            let effective = analysis.effective_agent();
            if effective != analysis.suggested_agent {
                warn!(
                    quality = analysis.quality.overall,
                    suggested = %analysis.suggested_agent,
                    "Low quality score — falling back to Claude"
                );
            }
            effective
        });

        // 3. Add user message
        session.messages.push(Message::user(user_message));

        // 4. Call agent
        debug!(agent = %agent_type, quality = analysis.quality.overall, "Calling agent");
        let call_result = self.router.call(agent_type.clone(), &session.messages, system_prompt).await;

        let mut result = match call_result {
            Ok(r) => {
                metrics::global().increment(metrics::names::AGENT_CALLS);
                r
            }
            Err(e) => {
                metrics::global().increment(metrics::names::AGENT_ERRORS);
                return Err(e);
            }
        };

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

        metrics::global().increment(metrics::names::DELTASHOTS_APPENDED);
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
        metrics::global().increment(metrics::names::SESSIONS_CREATED);
        info!(session_id = %session.session_id, "Session created");
        Ok(session)
    }

    /// Close a session.
    ///
    /// Applies a long fixed TTL (`CLOSED_SESSION_TTL_SECS`, default 30 days)
    /// to the Redis key — closed sessions receive no further writes, so the
    /// sliding window from `save_session` would not keep them alive. Then
    /// fires a background Notion sync of the final session state.
    pub async fn close_session(
        session_id: &str,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> PcwResult<()> {
        let mut session = Self::load_session(session_id, conn).await?;
        session.status = SessionStatus::Closed;
        session.closed_at = Some(now());
        Self::save_session(&session, conn).await?;

        // Override with longer TTL — closed sessions no longer slide
        let closed_ttl = pcw_core::config::get_settings().closed_session_ttl_secs;
        let key = key_session_meta(session_id);
        let _: () = conn
            .expire(&key, closed_ttl as i64)
            .await
            .map_err(|e| PcwError::RedisError(e.to_string()))?;

        metrics::global().increment(metrics::names::SESSIONS_CLOSED);

        // Fire-and-forget final Notion sync — does not block the API response
        let snapshot = session.clone();
        tokio::spawn(async move {
            if let Some(page_id) = infra::notion::sync_session(&snapshot).await {
                info!(session_id = %snapshot.session_id, notion_page = %page_id, "Session synced to Notion");
            }
        });

        Ok(())
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}
