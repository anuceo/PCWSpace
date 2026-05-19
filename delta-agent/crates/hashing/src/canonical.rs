use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

pub fn to_canonical_bytes<T: Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    let v = serde_json::to_value(value)?;
    serde_json::to_vec(&sort_keys(v))
}

fn sort_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted = map
                .into_iter()
                .map(|(k, v)| (k, sort_keys(v)))
                .collect::<BTreeMap<_, _>>();
            let mut out = serde_json::Map::new();
            for (k, v) in sorted {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_keys).collect()),
        other => other,
    }
}
