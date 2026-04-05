use serde::Serialize;

pub fn to_canonical_bytes<T: Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(value)
}
