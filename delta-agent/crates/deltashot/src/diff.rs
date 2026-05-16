use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::Value;

use crate::ops::{Op, OpType};

pub fn compute_diff(prev: &Value, next: &Value) -> Result<Vec<Op>> {
    compute_diff_ops(prev, next)
}

pub fn compute_diff_ops(prev: &Value, next: &Value) -> Result<Vec<Op>> {
    let mut ops = Vec::new();
    diff_recursive("root", prev, next, &mut ops)?;
    ops.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(ops)
}

fn diff_recursive(path: &str, prev: &Value, next: &Value, ops: &mut Vec<Op>) -> Result<()> {
    match (prev, next) {
        (Value::Object(prev_map), Value::Object(next_map)) => {
            // Deletions
            for key in prev_map.keys() {
                if !next_map.contains_key(key) {
                    ops.push(Op::new(format!("{path}.{}", key), OpType::Delete, None));
                }
            }

            // Additions + updates
            let ordered = next_map.keys().cloned().collect::<BTreeSet<_>>();
            for key in ordered {
                let next_val = next_map
                    .get(&key)
                    .expect("ordered keys sourced from same map");
                let new_path = format!("{path}.{key}");
                match prev_map.get(&key) {
                    None => ops.push(Op::new(new_path, OpType::Set, Some(next_val.clone()))),
                    Some(prev_val) => diff_recursive(&new_path, prev_val, next_val, ops)?,
                }
            }
        }
        (Value::Array(prev_arr), Value::Array(next_arr)) => {
            handle_array_diff(path, prev_arr, next_arr, ops)?;
        }
        _ => {
            if prev != next {
                ops.push(Op::new(
                    path.to_owned(),
                    OpType::Replace,
                    Some(next.clone()),
                ));
            }
        }
    }
    Ok(())
}

fn handle_array_diff(path: &str, prev: &[Value], next: &[Value], ops: &mut Vec<Op>) -> Result<()> {
    let min_len = prev.len().min(next.len());

    for i in 0..min_len {
        diff_recursive(&format!("{path}[{i}]"), &prev[i], &next[i], ops)?;
    }

    if next.len() > prev.len() {
        for item in next.iter().skip(prev.len()) {
            ops.push(Op::new(path.to_owned(), OpType::Append, Some(item.clone())));
        }
    }

    if prev.len() > next.len() {
        ops.push(Op::new(
            path.to_owned(),
            OpType::Replace,
            Some(Value::Array(next.to_vec())),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compute_diff, compute_diff_ops};
    use crate::apply::apply_ops;
    use crate::ops::OpType;

    #[test]
    fn computes_nested_set_replace_and_delete_ops() {
        let before = json!({
            "a": 1,
            "obj": {
                "x": "old",
                "y": 1
            }
        });
        let after = json!({
            "a": 2,
            "obj": {
                "x": "new"
            },
            "b": 3
        });

        let ops = compute_diff_ops(&before, &after).expect("diff should work");
        assert!(ops
            .iter()
            .any(|op| op.path == "root.a" && op.op_type == OpType::Replace));
        assert!(ops
            .iter()
            .any(|op| op.path == "root.obj.x" && op.op_type == OpType::Replace));
        assert!(ops
            .iter()
            .any(|op| op.path == "root.obj.y" && op.op_type == OpType::Delete));
        assert!(ops
            .iter()
            .any(|op| op.path == "root.b" && op.op_type == OpType::Set));
    }

    #[test]
    fn test_diff_and_apply() {
        let prev = json!({"a": 1});
        let next = json!({"a": 2, "b": 3});

        let ops = compute_diff(&prev, &next).expect("diff should compute");

        let mut state = prev.clone();
        apply_ops(&mut state, &ops).expect("ops should apply");

        assert_eq!(state, next);
    }
}
