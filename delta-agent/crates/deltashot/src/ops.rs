use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaOp {
    Set { value: String },
    Replace { from: String, to: String },
    Append { suffix: String },
    Remove { target: String },
}
