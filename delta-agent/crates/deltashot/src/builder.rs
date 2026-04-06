use crate::diff::compute_diff_ops;
use crate::ops::Op;

#[derive(Default)]
pub struct DeltaShotBuilder {
    base: serde_json::Value,
    next: serde_json::Value,
}

impl DeltaShotBuilder {
    pub fn new(base: serde_json::Value) -> Self {
        Self {
            base,
            next: serde_json::Value::Null,
        }
    }

    pub fn next(mut self, value: serde_json::Value) -> Self {
        self.next = value;
        self
    }

    pub fn build(self) -> Vec<Op> {
        compute_diff_ops(&self.base, &self.next).unwrap_or_default()
    }
}
