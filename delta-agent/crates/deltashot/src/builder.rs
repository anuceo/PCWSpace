use crate::diff::structured_diff;
use crate::ops::DeltaOp;

#[derive(Default)]
pub struct DeltaShotBuilder {
    base: String,
    next: String,
}

impl DeltaShotBuilder {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            next: String::new(),
        }
    }

    pub fn next(mut self, value: impl Into<String>) -> Self {
        self.next = value.into();
        self
    }

    pub fn build(self) -> Vec<DeltaOp> {
        structured_diff(&self.base, &self.next)
    }
}
