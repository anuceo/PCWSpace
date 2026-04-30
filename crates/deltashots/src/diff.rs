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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── compute_diff ──────────────────────────────────────────────────────

    #[test]
    fn identical_values_produce_empty_diff() {
        let v = json!({"a": 1, "b": "hello"});
        assert!(is_diff_empty(&compute_diff(&v, &v)));
    }

    #[test]
    fn added_field_detected() {
        let before = json!({"a": 1});
        let after  = json!({"a": 1, "b": 2});
        let diff = compute_diff(&before, &after);
        assert_eq!(diff["added"]["b"], json!(2));
        assert!(diff.get("removed").is_none());
    }

    #[test]
    fn removed_field_detected() {
        let before = json!({"a": 1, "b": 2});
        let after  = json!({"a": 1});
        let diff = compute_diff(&before, &after);
        assert_eq!(diff["removed"]["b"], json!(2));
        assert!(diff.get("added").is_none());
    }

    #[test]
    fn changed_field_detected() {
        let before = json!({"x": "old"});
        let after  = json!({"x": "new"});
        let diff = compute_diff(&before, &after);
        // Changed holds a nested diff for the field
        assert!(diff["changed"]["x"].is_object());
        assert!(diff.get("added").is_none());
        assert!(diff.get("removed").is_none());
    }

    #[test]
    fn nested_object_change_detected() {
        let before = json!({"user": {"score": 10}});
        let after  = json!({"user": {"score": 20}});
        let diff = compute_diff(&before, &after);
        // Changed → user → nested diff for score
        assert!(diff["changed"]["user"].is_object());
    }

    #[test]
    fn scalar_values_use_before_after_format() {
        let diff = compute_diff(&json!("v1"), &json!("v2"));
        assert_eq!(diff["before"], json!("v1"));
        assert_eq!(diff["after"],  json!("v2"));
    }

    // ── apply_diff ────────────────────────────────────────────────────────

    #[test]
    fn apply_adds_field() {
        let mut state = json!({"a": 1});
        apply_diff(&mut state, &json!({"added": {"b": 99}}));
        assert_eq!(state["b"], json!(99));
        assert_eq!(state["a"], json!(1));
    }

    #[test]
    fn apply_removes_field() {
        let mut state = json!({"a": 1, "b": 2});
        apply_diff(&mut state, &json!({"removed": {"b": 2}}));
        assert!(state.get("b").is_none());
        assert_eq!(state["a"], json!(1));
    }

    #[test]
    fn apply_changes_scalar_field() {
        let mut state = json!({"x": "old"});
        // Scalar changes use the before/after sub-diff
        apply_diff(&mut state, &json!({"changed": {"x": {"before": "old", "after": "new"}}}));
        assert_eq!(state["x"], json!("new"));
    }

    #[test]
    fn empty_diff_is_noop() {
        let mut state = json!({"keep": "this"});
        let original = state.clone();
        apply_diff(&mut state, &json!({}));
        assert_eq!(state, original);
    }

    // ── roundtrip ─────────────────────────────────────────────────────────

    #[test]
    fn roundtrip_add_remove_change() {
        let before = json!({"a": 1, "b": "old", "c": true});
        let after  = json!({"a": 1, "b": "new", "d": 99});
        let diff = compute_diff(&before, &after);
        let mut state = before.clone();
        apply_diff(&mut state, &diff);
        assert_eq!(state, after);
    }

    #[test]
    fn roundtrip_nested_objects() {
        let before = json!({"user": {"name": "Alice", "score": 10}, "active": true});
        let after  = json!({"user": {"name": "Alice", "score": 20}, "active": true, "badge": "gold"});
        let diff = compute_diff(&before, &after);
        let mut state = before.clone();
        apply_diff(&mut state, &diff);
        assert_eq!(state, after);
    }

    #[test]
    fn roundtrip_scalar_replace() {
        let before = json!("version_a");
        let after  = json!("version_b");
        let diff = compute_diff(&before, &after);
        let mut val = before.clone();
        apply_diff(&mut val, &diff);
        assert_eq!(val, after);
    }

    #[test]
    fn roundtrip_session_state_shape() {
        let before = json!({"session_id": "s1", "messages": [], "status": "active"});
        let after  = json!({
            "session_id": "s1",
            "messages": [{"role": "user", "content": "hi"}],
            "status": "active",
            "turn_count": 1
        });
        let diff = compute_diff(&before, &after);
        let mut state = before.clone();
        apply_diff(&mut state, &diff);
        assert_eq!(state, after);
    }

    // ── serialisation ─────────────────────────────────────────────────────

    #[test]
    fn diff_bytes_roundtrip() {
        let diff = json!({"added": {"x": 1}, "removed": {"y": 2}});
        let bytes = diff_to_bytes(&diff);
        assert_eq!(bytes_to_diff(&bytes).unwrap(), diff);
    }

    #[test]
    fn bytes_to_diff_rejects_invalid_json() {
        assert!(bytes_to_diff(b"not valid json {{{{").is_err());
    }
}
