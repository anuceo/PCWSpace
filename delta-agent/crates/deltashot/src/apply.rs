use anyhow::{anyhow, bail, Result};
use serde_json::Value;

use crate::ops::{Op, OpType};

pub fn apply_ops_to_state(base: &Value, ops: &[Op]) -> Result<Value> {
    let mut state = base.clone();
    for op in ops {
        apply_single_op(&mut state, op)?;
    }
    Ok(state)
}

fn apply_single_op(state: &mut Value, op: &Op) -> Result<()> {
    let segments = parse_json_pointer(&op.path)?;
    if segments.is_empty() {
        return apply_root_op(state, op);
    }

    let leaf = segments.last().expect("checked non-empty").to_string();
    let parent = ensure_parent_object(state, &segments[..segments.len() - 1])?;
    match op.op_type {
        OpType::Set | OpType::Replace => {
            parent.insert(leaf, op.value.clone().unwrap_or(Value::Null));
        }
        OpType::Append => {
            let incoming = op.value.clone().unwrap_or(Value::Null);
            if let Some(existing) = parent.get_mut(&leaf) {
                match (existing, incoming) {
                    (Value::Array(existing_arr), Value::Array(incoming_arr)) => {
                        existing_arr.extend(incoming_arr);
                    }
                    (Value::Array(existing_arr), other) => existing_arr.push(other),
                    (Value::String(existing_text), Value::String(suffix)) => {
                        existing_text.push_str(&suffix)
                    }
                    (slot, other) => *slot = other,
                }
            } else {
                parent.insert(leaf, incoming);
            }
        }
        OpType::Delete => {
            parent.remove(&leaf);
        }
    }
    Ok(())
}

fn apply_root_op(state: &mut Value, op: &Op) -> Result<()> {
    match op.op_type {
        OpType::Set | OpType::Replace | OpType::Append => {
            *state = op.value.clone().unwrap_or(Value::Null);
        }
        OpType::Delete => {
            *state = Value::Null;
        }
    }
    Ok(())
}

fn ensure_parent_object<'a>(
    root: &'a mut Value,
    parent_segments: &[String],
) -> Result<&'a mut serde_json::Map<String, Value>> {
    let mut current = root;
    for segment in parent_segments {
        if !current.is_object() {
            *current = Value::Object(serde_json::Map::new());
        }
        let map = current
            .as_object_mut()
            .ok_or_else(|| anyhow!("parent path is not an object"))?;
        current = map
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if !current.is_object() {
        *current = Value::Object(serde_json::Map::new());
    }
    current
        .as_object_mut()
        .ok_or_else(|| anyhow!("failed to resolve parent object"))
}

fn parse_json_pointer(path: &str) -> Result<Vec<String>> {
    if path.is_empty() {
        return Ok(Vec::new());
    }
    if !path.starts_with('/') {
        bail!("op path must be a JSON pointer starting with '/'");
    }
    Ok(path
        .trim_start_matches('/')
        .split('/')
        .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
        .collect())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::apply_ops_to_state;
    use crate::ops::{Op, OpType};

    #[test]
    fn applies_set_replace_append_delete_ops() {
        let base = json!({
            "goal": "draft",
            "items": ["a"],
            "msg": "hi"
        });
        let ops = vec![
            Op::new("/goal".to_owned(), OpType::Replace, Some(json!("final"))),
            Op::new("/items".to_owned(), OpType::Append, Some(json!("b"))),
            Op::new("/msg".to_owned(), OpType::Append, Some(json!(" there"))),
            Op::new("/obsolete".to_owned(), OpType::Delete, None),
        ];

        let next = apply_ops_to_state(&base, &ops).expect("ops should apply");
        assert_eq!(next.get("goal"), Some(&json!("final")));
        assert_eq!(next.get("items"), Some(&json!(["a", "b"])));
        assert_eq!(next.get("msg"), Some(&json!("hi there")));
    }
}
