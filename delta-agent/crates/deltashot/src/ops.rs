use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaShot {
    pub id: String,
    pub session_id: String,
    pub branch_id: String,
    pub prev_hash: String,
    pub hash: String,
    pub timestamp: u128,
    pub ops: Vec<Op>,
    pub artifacts: Vec<String>,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Op {
    pub path: String,
    pub op_type: OpType,
    pub value: Option<Value>,
}

impl Op {
    pub fn new(path: String, op_type: OpType, value: Option<Value>) -> Self {
        Self {
            path,
            op_type,
            value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OpType {
    Set,
    Replace,
    Append,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metadata {
    pub event_type: String,
    pub agent: Option<String>,
    pub workflow_step: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalOp {
    path: String,
    op_type: OpType,
    value: Option<Value>,
}

pub fn canonical_serialize_ops(ops: &[Op]) -> Result<Vec<u8>> {
    let mut canonical = ops
        .iter()
        .map(|op| CanonicalOp {
            path: op.path.clone(),
            op_type: op.op_type.clone(),
            value: op.value.as_ref().map(canonicalize_value),
        })
        .collect::<Vec<_>>();
    canonical.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then_with(|| op_sort_key(&a.op_type).cmp(&op_sort_key(&b.op_type)))
            .then_with(|| canonical_value_key(&a.value).cmp(&canonical_value_key(&b.value)))
    });
    Ok(serde_json::to_vec(&canonical)?)
}

fn op_sort_key(op: &OpType) -> u8 {
    match op {
        OpType::Set => 0,
        OpType::Replace => 1,
        OpType::Append => 2,
        OpType::Delete => 3,
    }
}

fn canonicalize_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let ordered = map
                .iter()
                .map(|(k, v)| (k.clone(), canonicalize_value(v)))
                .collect::<BTreeMap<_, _>>();
            let mut out = serde_json::Map::new();
            for (k, v) in ordered {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_value).collect()),
        _ => value.clone(),
    }
}

fn canonical_value_key(value: &Option<Value>) -> String {
    match value {
        Some(v) => serde_json::to_string(v).unwrap_or_default(),
        None => String::new(),
    }
}
