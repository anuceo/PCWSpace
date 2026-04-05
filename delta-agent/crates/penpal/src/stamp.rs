use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PenpalStamp {
    pub lattice_key: String,
    pub digest: String,
}

impl PenpalStamp {
    pub fn new(lattice_key: impl Into<String>, payload: &[u8]) -> Self {
        let lattice_key = lattice_key.into();
        let digest = blake3::hash(payload).to_hex().to_string();

        Self { lattice_key, digest }
    }
}
