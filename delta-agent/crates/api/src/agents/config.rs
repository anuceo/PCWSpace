use std::time::Duration;

use crate::agents::retry::RetryPolicy;

const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 4_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_RETRIES: u32 = 2;
const DEFAULT_RETRY_BASE_MS: u64 = 250;
const DEFAULT_RETRY_MAX_MS: u64 = 2_500;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const DEFAULT_ANTHROPIC_MODEL: &str = "claude-sonnet-4-6";
const DEFAULT_DEEPSEEK_BASE_URL: &str = "https://api.deepseek.com";
const DEFAULT_DEEPSEEK_MODEL: &str = "deepseek-chat";

#[derive(Debug, Clone)]
pub struct AgentRuntimeConfig {
    pub use_real_agents: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub anthropic_base_url: String,
    pub anthropic_api_key: Option<String>,
    pub anthropic_model: String,
    pub deepseek_base_url: String,
    pub deepseek_api_key: Option<String>,
    pub deepseek_model: String,
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            use_real_agents: false,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            stream_idle_timeout_ms: DEFAULT_STREAM_IDLE_TIMEOUT_MS,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base_ms: DEFAULT_RETRY_BASE_MS,
            retry_max_ms: DEFAULT_RETRY_MAX_MS,
            anthropic_base_url: DEFAULT_ANTHROPIC_BASE_URL.to_owned(),
            anthropic_api_key: None,
            anthropic_model: DEFAULT_ANTHROPIC_MODEL.to_owned(),
            deepseek_base_url: DEFAULT_DEEPSEEK_BASE_URL.to_owned(),
            deepseek_api_key: None,
            deepseek_model: DEFAULT_DEEPSEEK_MODEL.to_owned(),
        }
    }
}

impl AgentRuntimeConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        config.use_real_agents =
            read_bool_env("DELTA_AGENT_USE_REAL_AGENTS", config.use_real_agents);
        config.connect_timeout_ms =
            read_u64_env("AGENT_CONNECT_TIMEOUT_MS", config.connect_timeout_ms);
        config.request_timeout_ms = read_u64_env("AGENT_TIMEOUT_MS", config.request_timeout_ms);
        config.stream_idle_timeout_ms = read_u64_env(
            "AGENT_STREAM_IDLE_TIMEOUT_MS",
            config.stream_idle_timeout_ms,
        );
        config.max_retries = read_u32_env("AGENT_MAX_RETRIES", config.max_retries);
        config.retry_base_ms = read_u64_env("AGENT_RETRY_BASE_MS", config.retry_base_ms);
        config.retry_max_ms = read_u64_env("AGENT_RETRY_MAX_MS", config.retry_max_ms);
        config.anthropic_base_url =
            read_string_env("ANTHROPIC_BASE_URL").unwrap_or(config.anthropic_base_url);
        config.anthropic_api_key = read_string_env("ANTHROPIC_API_KEY");
        config.anthropic_model =
            read_string_env("ANTHROPIC_MODEL").unwrap_or(config.anthropic_model);
        config.deepseek_base_url =
            read_string_env("DEEPSEEK_BASE_URL").unwrap_or(config.deepseek_base_url);
        config.deepseek_api_key = read_string_env("DEEPSEEK_API_KEY");
        config.deepseek_model = read_string_env("DEEPSEEK_MODEL").unwrap_or(config.deepseek_model);
        config
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn stream_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.stream_idle_timeout_ms)
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_retries: self.max_retries,
            base_delay: Duration::from_millis(self.retry_base_ms),
            max_delay: Duration::from_millis(self.retry_max_ms.max(self.retry_base_ms)),
        }
    }
}

fn read_string_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_u64_env(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn read_u32_env(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

fn read_bool_env(key: &str, default: bool) -> bool {
    let Some(raw) = std::env::var(key).ok() else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}
