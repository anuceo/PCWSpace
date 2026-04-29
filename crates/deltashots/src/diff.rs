use serde_json::Value;

/// Compute a diff between two JSON values.
/// Returns an empty object if identical, otherwise a diff object.
/// Format: {"added": {...}, "removed": {...}, "changed": {...}}
pub fn compute_diff(before: &Value, after: &Value) -> Value {
    if before == after {
        return Value::Object(Default::default()); // empty = no change
    }
    match (before, after) {
        (Value::Object(b), Value::Object(a)) => {
            let mut added   = serde_json::Map::new();
            let mut removed = serde_json::Map::new();
            let mut changed = serde_json::Map::new();

            // Check removed and changed
            for (k, bv) in b {
                match a.get(k) {
                    None => { removed.insert(k.clone(), bv.clone()); }
                    Some(av) => {
                        if av != bv {
                            let nested = compute_diff(bv, av);
                            changed.insert(k.clone(), nested);
                        }
                    }
                }
            }
            // Check added
            for (k, av) in a {
                if !b.contains_key(k) {
                    added.insert(k.clone(), av.clone());
                }
            }

            let mut result = serde_json::Map::new();
            if !added.is_empty()   { result.insert("added".into(),   Value::Object(added)); }
            if !removed.is_empty() { result.insert("removed".into(), Value::Object(removed)); }
            if !changed.is_empty() { result.insert("changed".into(), Value::Object(changed)); }
            Value::Object(result)
        }
        _ => {
            // Non-object: just record before/after
            serde_json::json!({ "before": before, "after": after })
        }
    }
}

pub fn is_diff_empty(diff: &Value) -> bool {
    match diff {
        Value::Object(m) => m.is_empty(),
        _ => false,
    }
}

/// Apply a diff (produced by compute_diff) onto a base Value.
/// Mutates the base in place.
pub fn apply_diff(base: &mut Value, diff: &Value) {
    if is_diff_empty(diff) { return; }
    let (base_obj, diff_obj) = match (base, diff) {
        (Value::Object(b), Value::Object(d)) => (b, d),
        (base, diff) => {
            // scalar replace: diff has "before"/"after"
            if let Some(after) = diff.get("after") {
                *base = after.clone();
            }
            return;
        }
    };

    // Apply added
    if let Some(Value::Object(added)) = diff_obj.get("added") {
        for (k, v) in added { base_obj.insert(k.clone(), v.clone()); }
    }
    // Apply removed
    if let Some(Value::Object(removed)) = diff_obj.get("removed") {
        for k in removed.keys() { base_obj.remove(k); }
    }
    // Apply changed (recursive)
    if let Some(Value::Object(changed)) = diff_obj.get("changed") {
        for (k, sub_diff) in changed {
            if let Some(field) = base_obj.get_mut(k) {
                apply_diff(field, sub_diff);
            }
        }
    }
}

pub fn diff_to_bytes(diff: &Value) -> Vec<u8> {
    serde_json::to_vec(diff).unwrap_or_default()
}

pub fn bytes_to_diff(bytes: &[u8]) -> pcw_core::errors::PcwResult<Value> {
    serde_json::from_slice(bytes).map_err(|e| pcw_core::errors::PcwError::SerializationError(e.to_string()))
}
