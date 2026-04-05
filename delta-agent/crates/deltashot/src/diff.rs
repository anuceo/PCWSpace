use crate::ops::DeltaOp;

pub fn structured_diff(before: &str, after: &str) -> Vec<DeltaOp> {
    if before == after {
        return Vec::new();
    }

    vec![DeltaOp::Replace {
        from: before.to_owned(),
        to: after.to_owned(),
    }]
}
