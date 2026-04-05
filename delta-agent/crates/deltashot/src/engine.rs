use crate::apply::apply_ops;
use crate::builder::DeltaShotBuilder;
use crate::ops::DeltaOp;

#[derive(Default)]
pub struct DeltaShotEngine;

impl DeltaShotEngine {
    pub fn build_delta(&self, base: &str, next: &str) -> Vec<DeltaOp> {
        DeltaShotBuilder::new(base).next(next).build()
    }

    pub fn apply_delta(&self, base: &str, ops: &[DeltaOp]) -> String {
        apply_ops(base, ops)
    }
}
