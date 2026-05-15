use std::collections::HashSet;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use tracing::warn;
use uuid::Uuid;

use crate::pipeline::{ApiErrorBody, ApiErrorEnvelope, ApiResponseMeta};

const X_API_KEY_HEADER: &str = "x-api-key";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthRole {
    Reader,
    Writer,
    Admin,
}

impl AuthRole {
    fn allows(self, policy: EndpointPolicy) -> bool {
        match self {
            Self::Admin => true,
            Self::Writer => !matches!(policy, EndpointPolicy::Admin),
            Self::Reader => matches!(policy, EndpointPolicy::Public | EndpointPolicy::Read),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub role: AuthRole,
    pub key_fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    enabled: bool,
    reader_keys: HashSet<String>,
    writer_keys: HashSet<String>,
    admin_keys: HashSet<String>,
}

impl AuthConfig {
    pub fn from_env() -> Self {
        let mut reader_keys = load_key_list("DELTA_AGENT_READONLY_API_KEYS");
        add_optional_key(
            &mut reader_keys,
            env::var("DELTA_AGENT_READONLY_API_KEY").ok(),
        );

        let mut writer_keys = load_key_list("DELTA_AGENT_API_KEYS");
        add_optional_key(&mut writer_keys, env::var("DELTA_AGENT_API_KEY").ok());
        add_optional_key(&mut writer_keys, env::var("PCW_API_KEY").ok());

        let mut admin_keys = load_key_list("DELTA_AGENT_ADMIN_API_KEYS");
        add_optional_key(&mut admin_keys, env::var("DELTA_AGENT_ADMIN_API_KEY").ok());

        let required = parse_env_bool("DELTA_AGENT_AUTH_REQUIRED", false);
        let disabled = parse_env_bool("DELTA_AGENT_AUTH_DISABLED", false);
        let has_keys = !(reader_keys.is_empty() && writer_keys.is_empty() && admin_keys.is_empty());
        let enabled = !disabled && (required || has_keys);

        if enabled && required && !has_keys {
            warn!(
                "auth is required but no keys were configured; all requests will be unauthorized"
            );
        }

        Self {
            enabled,
            reader_keys,
            writer_keys,
            admin_keys,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn authenticate(&self, api_key: &str) -> Option<AuthRole> {
        if self.admin_keys.contains(api_key) {
            return Some(AuthRole::Admin);
        }
        if self.writer_keys.contains(api_key) {
            return Some(AuthRole::Writer);
        }
        if self.reader_keys.contains(api_key) {
            return Some(AuthRole::Reader);
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointPolicy {
    Public,
    Read,
    Write,
    Admin,
}

impl EndpointPolicy {
    fn from_request(method: &Method, path: &str) -> Self {
        if method == Method::OPTIONS || path == "/health" || path.ends_with("/health") {
            return Self::Public;
        }
        if path.contains("/debug/") {
            return Self::Admin;
        }
        if *method == Method::GET || *method == Method::HEAD {
            return Self::Read;
        }
        Self::Write
    }
}

pub async fn authentication_middleware(
    State(config): State<AuthConfig>,
    mut request: Request,
    next: Next,
) -> Response {
    let policy = EndpointPolicy::from_request(request.method(), request.uri().path());

    if !config.is_enabled() || matches!(policy, EndpointPolicy::Public) {
        return next.run(request).await;
    }

    let Some(api_key) = extract_api_key(request.headers()) else {
        return auth_error_response(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing API credentials",
        );
    };

    let Some(role) = config.authenticate(&api_key) else {
        return auth_error_response(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "invalid API credentials",
        );
    };

    if !role.allows(policy) {
        return auth_error_response(
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "insufficient permissions for this endpoint",
        );
    }

    request.extensions_mut().insert(AuthContext {
        role,
        key_fingerprint: fingerprint_key(&api_key),
    });
    next.run(request).await
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(AUTHORIZATION).and_then(|header| header.to_str().ok()) {
        let trimmed = value.trim();
        if let Some(token) = trimmed.strip_prefix("Bearer ") {
            let key = token.trim();
            if !key.is_empty() {
                return Some(key.to_owned());
            }
        }
    }
    headers
        .get(X_API_KEY_HEADER)
        .and_then(|header| header.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn auth_error_response(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let envelope = ApiErrorEnvelope {
        error: ApiErrorBody {
            code: code.to_owned(),
            message: message.to_owned(),
            retryable: false,
        },
        meta: ApiResponseMeta {
            request_id: format!("req_{}", Uuid::new_v4().simple()),
            timestamp: now_ms(),
            latency_ms: 0,
        },
    };
    (status, Json(envelope)).into_response()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_millis() as u64
}

fn load_key_list(env_name: &str) -> HashSet<String> {
    env::var(env_name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(ToOwned::to_owned)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn add_optional_key(target: &mut HashSet<String>, value: Option<String>) {
    if let Some(value) = value {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            target.insert(trimmed.to_owned());
        }
    }
}

fn parse_env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn fingerprint_key(key: &str) -> String {
    let digest = blake3::hash(key.as_bytes()).to_hex().to_string();
    digest.chars().take(12).collect::<String>()
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, Method};

    use super::{extract_api_key, AuthRole, EndpointPolicy};

    #[test]
    fn extracts_api_key_from_bearer_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-token"),
        );
        assert_eq!(
            extract_api_key(&headers).as_deref(),
            Some("secret-token"),
            "bearer token should be parsed"
        );
    }

    #[test]
    fn extracts_api_key_from_x_api_key_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("abc123"));
        assert_eq!(extract_api_key(&headers).as_deref(), Some("abc123"));
    }

    #[test]
    fn classifies_debug_paths_as_admin_policy() {
        let policy = EndpointPolicy::from_request(&Method::POST, "/api/v1/debug/sessions/x/audit");
        assert_eq!(policy, EndpointPolicy::Admin);
    }

    #[test]
    fn role_permissions_match_policy_rules() {
        assert!(AuthRole::Reader.allows(EndpointPolicy::Read));
        assert!(!AuthRole::Reader.allows(EndpointPolicy::Write));
        assert!(AuthRole::Writer.allows(EndpointPolicy::Write));
        assert!(!AuthRole::Writer.allows(EndpointPolicy::Admin));
        assert!(AuthRole::Admin.allows(EndpointPolicy::Admin));
    }
}
