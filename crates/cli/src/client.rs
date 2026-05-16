use reqwest::Client;
use serde_json::Value;

pub struct PcwClient {
    base_url: String,
    api_key: String,
    http: Client,
}

impl PcwClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            http: Client::new(),
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .get(&url)
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| format!("Parse error: {e}"))?;

        if !status.is_success() {
            let err = body["error"].as_str().unwrap_or("Unknown error");
            return Err(format!("HTTP {status}: {err}"));
        }
        Ok(body)
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| format!("Parse error: {e}"))?;

        if !status.is_success() {
            let err = body["error"].as_str().unwrap_or("Unknown error");
            return Err(format!("HTTP {status}: {err}"));
        }
        Ok(body)
    }

    pub async fn health(&self) -> Result<Value, String> {
        let url = format!("{}/health", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        resp.json().await.map_err(|e| format!("Parse error: {e}"))
    }
}
