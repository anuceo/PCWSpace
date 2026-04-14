pub mod apply;
pub mod diff;
pub mod engine;
pub mod ops;

pub use apply::{apply_ops, apply_ops_to_state};
pub use diff::{compute_diff, compute_diff_ops};
pub use engine::{compute_chain_hash, verify_hash_chain, DeltaShotEngine};
pub use ops::{canonical_serialize_ops, DeltaShot, Metadata, Op, OpType};

pub type DeltaShotOp = Op;
pub type DeltaShotMetadata = Metadata;
