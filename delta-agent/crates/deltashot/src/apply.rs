use anyhow::{anyhow, bail, Result};
use serde_json::Value;

use crate::ops::{Op, OpType};

pub fn apply_ops(state: &mut Value, ops: &[Op]) -> Result<()> {
    for op in ops {
        apply_single_op(state, op)?;
    }
    Ok(())
}

pub fn apply_ops_to_state(base: &Value, ops: &[Op]) -> Result<Value> {
    let mut state = base.clone();
    apply_ops(&mut state, ops)?;
    Ok(state)
}

fn apply_single_op(state: &mut Value, op: &Op) -> Result<()> {
    match op.op_type {
        OpType::Set | OpType::Replace => {
            set_value_at_path(state, &op.path, op.value.clone())?;
        }
        OpType::Delete => {
            delete_value_at_path(state, &op.path)?;
        }
        OpType::Append => {
            append_value_at_path(state, &op.path, op.value.clone())?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathToken {
    Key(String),
    Index(usize),
}

fn set_value_at_path(root: &mut Value, path: &str, value: Option<Value>) -> Result<()> {
    let tokens = parse_path(path)?;
    if tokens.is_empty() {
        *root = value.unwrap_or(Value::Null);
        return Ok(());
    }

    let mut current = root;
    for token in &tokens[..tokens.len() - 1] {
        current = match token {
            PathToken::Key(key) => {
                if !current.is_object() {
                    *current = Value::Object(serde_json::Map::new());
                }
                let map = current
                    .as_object_mut()
                    .ok_or_else(|| anyhow!("failed to coerce object path"))?;
                map.entry(key.clone())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()))
            }
            PathToken::Index(index) => {
                let arr = current
                    .as_array_mut()
                    .ok_or_else(|| anyhow!("path index points to non-array"))?;
                arr.get_mut(*index)
                    .ok_or_else(|| anyhow!("array index out of bounds"))?
            }
        };
    }

    match tokens.last().expect("tokens not empty") {
        PathToken::Key(key) => {
            if !current.is_object() {
                *current = Value::Object(serde_json::Map::new());
            }
            let map = current
                .as_object_mut()
                .ok_or_else(|| anyhow!("failed to resolve object leaf"))?;
            map.insert(key.clone(), value.unwrap_or(Value::Null));
        }
        PathToken::Index(index) => {
            let arr = current
                .as_array_mut()
                .ok_or_else(|| anyhow!("path index points to non-array"))?;
            if *index >= arr.len() {
                bail!("array index out of bounds");
            }
            arr[*index] = value.unwrap_or(Value::Null);
        }
    }
    Ok(())
}

fn delete_value_at_path(root: &mut Value, path: &str) -> Result<()> {
    let tokens = parse_path(path)?;
    if tokens.is_empty() {
        *root = Value::Null;
        return Ok(());
    }

    let mut current = root;
    for token in &tokens[..tokens.len() - 1] {
        current = match token {
            PathToken::Key(key) => current
                .get_mut(key)
                .ok_or_else(|| anyhow!("key '{}' missing while deleting", key))?,
            PathToken::Index(index) => current
                .get_mut(*index)
                .ok_or_else(|| anyhow!("index {} missing while deleting", index))?,
        };
    }

    match tokens.last().expect("tokens not empty") {
        PathToken::Key(key) => {
            let map = current
                .as_object_mut()
                .ok_or_else(|| anyhow!("delete target parent is not object"))?;
            map.remove(key);
        }
        PathToken::Index(index) => {
            let arr = current
                .as_array_mut()
                .ok_or_else(|| anyhow!("delete target parent is not array"))?;
            if *index >= arr.len() {
                bail!("array index out of bounds");
            }
            arr.remove(*index);
        }
    }
    Ok(())
}

fn append_value_at_path(root: &mut Value, path: &str, value: Option<Value>) -> Result<()> {
    let tokens = parse_path(path)?;
    if tokens.is_empty() {
        bail!("append path cannot be root");
    }

    let mut current = root;
    for token in &tokens {
        current = match token {
            PathToken::Key(key) => {
                if !current.is_object() {
                    *current = Value::Object(serde_json::Map::new());
                }
                let map = current
                    .as_object_mut()
                    .ok_or_else(|| anyhow!("append path parent is not object"))?;
                map.entry(key.clone())
                    .or_insert_with(|| Value::Array(Vec::new()))
            }
            PathToken::Index(index) => {
                let arr = current
                    .as_array_mut()
                    .ok_or_else(|| anyhow!("append path index parent is not array"))?;
                arr.get_mut(*index)
                    .ok_or_else(|| anyhow!("append index out of bounds"))?
            }
        };
    }

    let arr = current
        .as_array_mut()
        .ok_or_else(|| anyhow!("append target is not array"))?;
    arr.push(value.unwrap_or(Value::Null));
    Ok(())
}

fn parse_path(path: &str) -> Result<Vec<PathToken>> {
    if path.trim().is_empty() || path == "root" {
        return Ok(Vec::new());
    }
    if !path.starts_with("root") {
        bail!("path must start with 'root'");
    }

    let bytes = path.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 4; // skip "root"
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                    i += 1;
                }
                if start == i {
                    bail!("empty key segment in path");
                }
                tokens.push(PathToken::Key(path[start..i].to_owned()));
            }
            b'[' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                if i >= bytes.len() {
                    bail!("unterminated index segment");
                }
                let idx = path[start..i]
                    .parse::<usize>()
                    .map_err(|_| anyhow!("invalid array index"))?;
                tokens.push(PathToken::Index(idx));
                i += 1; // skip ']'
            }
            _ => bail!("invalid path token"),
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::apply_ops;
    use crate::ops::{Op, OpType};

    #[test]
    fn applies_set_replace_append_delete_ops() {
        let base = json!({
            "goal": "draft",
            "items": ["a"],
            "obsolete": true
        });
        let ops = vec![
            Op::new(
                "root.goal".to_owned(),
                OpType::Replace,
                Some(json!("final")),
            ),
            Op::new("root.items".to_owned(), OpType::Append, Some(json!("b"))),
            Op::new("root.obsolete".to_owned(), OpType::Delete, None),
        ];

        let mut next = base.clone();
        apply_ops(&mut next, &ops).expect("ops should apply");
        assert_eq!(next.get("goal"), Some(&json!("final")));
        assert_eq!(next.get("items"), Some(&json!(["a", "b"])));
        assert!(next.get("obsolete").is_none());
    }

    #[test]
    fn applies_diff_to_exact_reconstruction() {
        let prev = json!({"a": 1});
        let next = json!({"a": 2, "b": 3});
        let ops = crate::diff::compute_diff(&prev, &next).expect("diff should compute");
        let mut state = prev.clone();
        apply_ops(&mut state, &ops).expect("apply should work");
        assert_eq!(state, next);
    }
}
