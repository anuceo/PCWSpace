/// Storm topology configuration and Nimbus REST submission.
///
/// Builds the topology JSON that Storm's Nimbus REST API expects, and provides
/// a client for submitting/killing topologies.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ── Topology spec ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentSpec {
    /// Shell command Storm uses to launch this component.
    pub command: Vec<String>,
    /// Number of parallel executor instances.
    pub parallelism: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamGrouping {
    pub stream_id: String,
    /// "shuffle", "fields", "all", "direct", "none"
    pub grouping:  String,
    /// Used when grouping = "fields"
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub fields:    Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoltSpec {
    pub component:  ComponentSpec,
    /// (source_component_id → grouping)
    pub inputs:     Vec<(String, StreamGrouping)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Topology {
    pub name:   String,
    pub spouts: std::collections::HashMap<String, ComponentSpec>,
    pub bolts:  std::collections::HashMap<String, BoltSpec>,
    /// Arbitrary Storm config overrides (workers, max spout pending, etc.)
    pub config: std::collections::HashMap<String, Value>,
}

impl Topology {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), ..Default::default() }
    }

    pub fn add_spout(mut self, id: impl Into<String>, spec: ComponentSpec) -> Self {
        self.spouts.insert(id.into(), spec);
        self
    }

    pub fn add_bolt(mut self, id: impl Into<String>, spec: BoltSpec) -> Self {
        self.bolts.insert(id.into(), spec);
        self
    }

    pub fn set_config(mut self, key: impl Into<String>, value: Value) -> Self {
        self.config.insert(key.into(), value);
        self
    }

    /// Serialise to the JSON structure expected by the Nimbus REST API
    /// (`POST /api/v1/topology/submit`).
    pub fn to_nimbus_json(&self) -> Value {
        let spouts: Value = self.spouts.iter().map(|(id, spec)| {
            json!({
                "id": id,
                "command": spec.command,
                "parallelism": spec.parallelism,
            })
        }).collect::<Vec<_>>().into();

        let bolts: Value = self.bolts.iter().map(|(id, b)| {
            let inputs: Value = b.inputs.iter().map(|(src, g)| json!({
                "component": src,
                "stream": g.stream_id,
                "grouping": g.grouping,
                "fields": g.fields,
            })).collect::<Vec<_>>().into();
            json!({
                "id": id,
                "command": b.component.command,
                "parallelism": b.component.parallelism,
                "inputs": inputs,
            })
        }).collect::<Vec<_>>().into();

        json!({
            "name":   self.name,
            "spouts": spouts,
            "bolts":  bolts,
            "config": self.config,
        })
    }
}

// ── Nimbus REST client ────────────────────────────────────────────────────────

pub struct NimbusClient {
    base_url: String,
    client:   reqwest::Client,
}

impl NimbusClient {
    pub fn new(nimbus_ui_url: impl Into<String>) -> Self {
        Self {
            base_url: nimbus_ui_url.into(),
            client:   reqwest::Client::new(),
        }
    }

    /// Submit a topology. Storm's Nimbus REST API accepts multipart/form-data
    /// with the topology jar + config, but for shell-command topologies the
    /// lightweight JSON submit endpoint is used via the Storm REST UI.
    pub async fn submit(&self, topology: &Topology) -> Result<(), String> {
        let url  = format!("{}/api/v1/topology/submit", self.base_url);
        let body = topology.to_nimbus_json();
        let resp = self.client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let msg = resp.text().await.unwrap_or_default();
            Err(format!("Nimbus submit failed: {msg}"))
        }
    }

    /// Kill a running topology by name (waits `wait_secs` before kill).
    pub async fn kill(&self, name: &str, wait_secs: u32) -> Result<(), String> {
        let url = format!("{}/api/v1/topology/{name}/kill/{wait_secs}", self.base_url);
        let resp = self.client.post(&url).send().await.map_err(|e| e.to_string())?;
        if resp.status().is_success() { Ok(()) } else {
            Err(format!("Kill failed: {}", resp.text().await.unwrap_or_default()))
        }
    }

    /// List running topologies.
    pub async fn list(&self) -> Result<Value, String> {
        let url = format!("{}/api/v1/topology/summary", self.base_url);
        self.client.get(&url).send().await
            .map_err(|e| e.to_string())?
            .json::<Value>().await
            .map_err(|e| e.to_string())
    }
}
