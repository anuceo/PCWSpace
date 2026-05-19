use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_runtime_env")]
    pub env: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PersistenceConfig {
    #[serde(default)]
    pub redis_url: Option<String>,
    #[serde(default)]
    pub vddab_root: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub required: Option<bool>,
    #[serde(default)]
    pub disabled: Option<bool>,
    #[serde(default)]
    pub writer_keys: Vec<String>,
    #[serde(default)]
    pub reader_keys: Vec<String>,
    #[serde(default)]
    pub admin_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AgentConfig {
    #[serde(default)]
    pub use_real_agents: Option<bool>,
    #[serde(default)]
    pub anthropic_api_key: Option<String>,
    #[serde(default)]
    pub anthropic_model: Option<String>,
    #[serde(default)]
    pub deepseek_api_key: Option<String>,
    #[serde(default)]
    pub deepseek_model: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NotionConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub database_id: Option<String>,
    #[serde(default)]
    pub parent_page_id: Option<String>,
    #[serde(default)]
    pub title_property: Option<String>,
    #[serde(default)]
    pub api_base_url: Option<String>,
    #[serde(default)]
    pub api_version: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub agents: AgentConfig,
    #[serde(default)]
    pub notion: NotionConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            env: default_runtime_env(),
        }
    }
}

pub fn load(path: impl AsRef<Path>) -> anyhow::Result<AppConfig> {
    let path_ref = path.as_ref();
    let contents = fs::read_to_string(path_ref)
        .with_context(|| format!("failed reading config {}", path_ref.display()))?;
    let parsed: AppConfig = toml::from_str(&contents)
        .with_context(|| format!("failed parsing config {}", path_ref.display()))?;
    Ok(parsed)
}

fn default_runtime_env() -> String {
    "prod".to_owned()
}
