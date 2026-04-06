use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::ops::{Op, OpType};

pub fn compute_diff_ops(before: &Value, after: &Value) -> Vec<Op> {
    let mut ops = Vec::new();
    diff_values("", before, after, &mut ops);
    ops
}

fn diff_values(path: &str, before: &Value, after: &Value, ops: &mut Vec<Op>) {
    match (before, after) {
        (Value::Object(before_map), Value::Object(after_map)) => {
            diff_objects(path, before_map, after_map, ops);
        }
        (Value::Array(before_items), Value::Array(after_items)) => {
            if before_items != after_items {
                if path.is_empty() {
                    ops.push(Op::new(
                        path.to_owned(),
                        OpType::Replace,
                        Some(Value::Array(after_items.clone())),
                    ));
                } else if before_items.len() < after_items.len()
                    && before_items == &after_items[..before_items.len()]
                {
                    for item in after_items.iter().skip(before_items.len()) {
                        ops.push(Op::new(path.to_owned(), OpType::Append, Some(item.clone())));
                    }
                } else {
                    ops.push(Op::new(
                        path.to_owned(),
                        OpType::Replace,
                        Some(Value::Array(after_items.clone())),
                    ));
                }
            }
        }
        _ => {
            if before != after {
                let op = if before.is_null() {
                    OpType::Set
                } else {
                    OpType::Replace
                };
                ops.push(Op::new(path.to_owned(), op, Some(after.clone())));
            }
        }
    }
}

fn diff_objects(
    path: &str,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
    ops: &mut Vec<Op>,
) {
    let mut keys = BTreeSet::new();
    for key in before.keys() {
        keys.insert(key.clone());
    }
    for key in after.keys() {
        keys.insert(key.clone());
    }

    for key in keys {
        let next_path = join_path(path, &key);
        match (before.get(&key), after.get(&key)) {
            (Some(previous), Some(current)) => {
                diff_values(&next_path, previous, current, ops);
            }
            (None, Some(current)) => {
                ops.push(Op::new(next_path, OpType::Set, Some(current.clone())));
            }
            (Some(_), None) => {
                ops.push(Op::new(next_path, OpType::Delete, None));
            }
            (None, None) => {}
        }
    }
}

fn join_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        format!("/{}", escape_json_pointer(key))
    } else {
        format!("{}/{}", parent, escape_json_pointer(key))
    }
}

fn escape_json_pointer(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::compute_diff_ops;
    use crate::ops::OpType;

    #[test]
    fn computes_nested_set_replace_and_delete_ops() {
        let before = json!({
            "goal": "draft",
            "summary": {
                "step": "draft",
                "owner": "agent"
            }
        });
        let after = json!({
            "goal": "final",
            "summary": {
                "step": "refine"
            },
            "version": 2
        });

        let ops = compute_diff_ops(&before, &after);
        assert!(ops
            .iter()
            .any(|op| op.path == "/goal" && op.op_type == OpType::Replace));
        assert!(ops
            .iter()
            .any(|op| op.path == "/summary/step" && op.op_type == OpType::Replace));
        assert!(ops
            .iter()
            .any(|op| op.path == "/summary/owner" && op.op_type == OpType::Delete));
        assert!(ops
            .iter()
            .any(|op| op.path == "/version" && op.op_type == OpType::Set));
    }
}
