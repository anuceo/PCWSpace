use crate::ops::DeltaOp;

pub fn apply_ops(base: &str, ops: &[DeltaOp]) -> String {
    let mut current = base.to_owned();

    for op in ops {
        match op {
            DeltaOp::Set { value } => current = value.clone(),
            DeltaOp::Replace { from, to } => {
                if &current == from {
                    current = to.clone();
                }
            }
            DeltaOp::Append { suffix } => current.push_str(suffix),
            DeltaOp::Remove { target } => {
                current = current.replace(target, "");
            }
        }
    }

    current
}
