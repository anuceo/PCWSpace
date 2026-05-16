use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LatticeCoordinate {
    pub x: i64,
    pub y: i64,
    pub z: i64,
}
