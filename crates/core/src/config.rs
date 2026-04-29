use crate::errors::PcwResult;
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct Settings {
    // Redis
    pub redis_url: String,

    // Anthropic
    pub anthropic_api_key: String,
    pub claude_model: String,

    // DeepSeek
    pub deepseek_api_key: String,
    pub deepseek_base_url: String,
    pub deepseek_model: String,

    // Notion
    pub notion_token: String,
    pub notion_database_id: String,
    pub notion_artifacts_database_id: String,

    // API
    pub pcw_api_key: String,
    pub pcw_host: String,
    pub pcw_port: u16,
    pub pcw_log_level: String,

    // Security
    pub session_key_ttl_secs: u64,
}

impl Settings {
    pub fn from_env() -> PcwResult<Self> {
        // Load .env file if present (ignore error if missing)
        let _ = dotenvy::dotenv();

        fn env(key: &str) -> String {
            std::env::var(key).unwrap_or_default()
        }

        fn env_or(key: &str, default: &str) -> String {
            std::env::var(key).unwrap_or_else(|_| default.to_string())
        }

        fn env_u16(key: &str, default: u16) -> u16 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        Ok(Self {
            redis_url:                    env_or("REDIS_URL", "redis://127.0.0.1:6379"),
            anthropic_api_key:            env("ANTHROPIC_API_KEY"),
            claude_model:                 env_or("CLAUDE_MODEL", "claude-sonnet-4-6"),
            deepseek_api_key:             env("DEEPSEEK_API_KEY"),
            deepseek_base_url:            env_or("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            deepseek_model:               env_or("DEEPSEEK_MODEL", "deepseek-chat"),
            notion_token:                 env("NOTION_TOKEN"),
            notion_database_id:           env("NOTION_DATABASE_ID"),
            notion_artifacts_database_id: env("NOTION_ARTIFACTS_DATABASE_ID"),
            pcw_api_key:                  env_or("PCW_API_KEY", "dev-insecure"),
            pcw_host:                     env_or("PCW_HOST", "0.0.0.0"),
            pcw_port:                     env_u16("PCW_PORT", 8000),
            pcw_log_level:                env_or("PCW_LOG_LEVEL", "info"),
            session_key_ttl_secs:         env_u64("SESSION_KEY_TTL_SECS", 86400),
        })
    }
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

pub fn get_settings() -> &'static Settings {
    SETTINGS.get_or_init(|| {
        Settings::from_env().expect("Failed to load settings")
    })
}
