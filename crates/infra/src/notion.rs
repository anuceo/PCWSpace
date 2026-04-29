use pcw_core::errors::{PcwError, PcwResult};
use pcw_core::models::{Artifact, Session};
use reqwest::Client;
use serde_json::{json, Value};

// ── NotionClient ───────────────────────────────────────────────────────────

pub struct NotionClient {
    client: Client,
    token: String,
    base_url: String,
}

impl NotionClient {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            token: token.into(),
            base_url: "https://api.notion.com/v1".into(),
        }
    }

    pub fn from_env() -> Self {
        Self::new(pcw_core::config::get_settings().notion_token.clone())
    }

    async fn post_with_retry(&self, path: &str, body: &Value) -> PcwResult<Value> {
        let url = format!("{}/{path}", self.base_url);
        let mut delay_ms = 2_000u64;

        for attempt in 0..4u32 {
            let resp = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("Notion-Version", "2022-06-28")
                .header("Content-Type", "application/json")
                .json(body)
                .send()
                .await
                .map_err(|e| PcwError::NotionSyncError(e.to_string()))?;

            if resp.status() == 429 && attempt < 3 {
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                delay_ms *= 2;
                continue;
            }

            if !resp.status().is_success() {
                let msg = resp.text().await.unwrap_or_default();
                return Err(PcwError::NotionSyncError(msg));
            }

            return resp
                .json::<Value>()
                .await
                .map_err(|e| PcwError::NotionSyncError(e.to_string()));
        }

        Err(PcwError::NotionSyncError("retry exhausted".into()))
    }

    /// Create a page in a database. Returns the new page's id.
    pub async fn create_page(
        &self,
        database_id: &str,
        properties: Value,
        children: Vec<Value>,
    ) -> PcwResult<String> {
        let body = json!({
            "parent": { "database_id": database_id },
            "properties": properties,
            "children": children,
        });
        let resp = self.post_with_retry("pages", &body).await?;
        resp["id"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| PcwError::NotionSyncError("no id in response".into()))
    }

    /// Append blocks to an existing page.
    pub async fn append_blocks(&self, page_id: &str, children: Vec<Value>) -> PcwResult<()> {
        let body = json!({ "children": children });
        self.post_with_retry(&format!("blocks/{page_id}/children"), &body)
            .await?;
        Ok(())
    }
}

// ── Block helpers ──────────────────────────────────────────────────────────

fn text_block(text: &str) -> Value {
    json!({
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{ "type": "text", "text": { "content": text } }]
        }
    })
}

fn heading_block(text: &str, level: u8) -> Value {
    let t = format!("heading_{level}");
    json!({
        "object": "block",
        "type": t,
        t: {
            "rich_text": [{ "type": "text", "text": { "content": text } }]
        }
    })
}

fn code_block(code: &str, language: &str) -> Value {
    json!({
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": [{ "type": "text", "text": { "content": code } }],
            "language": language
        }
    })
}

// ── Free functions ─────────────────────────────────────────────────────────

/// Sync a Session to Notion. Returns the created page_id, or `None` if Notion
/// is not configured or the sync fails.
pub async fn sync_session(session: &Session) -> Option<String> {
    let settings = pcw_core::config::get_settings();
    if settings.notion_token.is_empty() || settings.notion_database_id.is_empty() {
        return None;
    }

    let client = NotionClient::from_env();

    let properties = json!({
        "Name": {
            "title": [{
                "type": "text",
                "text": { "content": format!("Session {}", &session.session_id[..8]) }
            }]
        },
        "Status": { "select": { "name": format!("{:?}", session.status) } },
        "Messages": { "number": session.messages.len() as i64 },
    });

    let mut children = vec![
        heading_block(&format!("Session: {}", session.session_id), 2),
        text_block(&format!(
            "Status: {:?} | Messages: {}",
            session.status,
            session.messages.len()
        )),
    ];

    // Show up to the last 20 messages (preserve chronological order).
    for msg in session.messages.iter().rev().take(20).rev() {
        let label = match &msg.agent_type {
            Some(a) => format!("[{}] via {a}:", msg.role),
            None => format!("[{}]:", msg.role),
        };
        let snippet = &msg.content[..msg.content.len().min(500)];
        children.push(text_block(&format!("{label} {snippet}")));
    }

    // Notion allows at most 100 children per request.
    children.truncate(100);

    client
        .create_page(&settings.notion_database_id, properties, children)
        .await
        .ok()
}

/// Sync an Artifact to Notion. Returns the created page_id, or `None` if
/// Notion is not configured or the sync fails.
pub async fn sync_artifact(artifact: &Artifact) -> Option<String> {
    let settings = pcw_core::config::get_settings();
    if settings.notion_token.is_empty() || settings.notion_artifacts_database_id.is_empty() {
        return None;
    }

    let client = NotionClient::from_env();

    let properties = json!({
        "Name": {
            "title": [{
                "type": "text",
                "text": { "content": format!("{} v{}", artifact.name, artifact.version) }
            }]
        },
        "Type": { "select": { "name": format!("{:?}", artifact.artifact_type) } },
        "Version": { "number": artifact.version as i64 },
    });

    let children = vec![
        heading_block(&format!("Artifact: {} v{}", artifact.name, artifact.version), 3),
        code_block(
            &artifact.content[..artifact.content.len().min(2000)],
            "plain text",
        ),
    ];

    client
        .create_page(&settings.notion_artifacts_database_id, properties, children)
        .await
        .ok()
}
