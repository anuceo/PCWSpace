use serde::Serialize;

use crate::canonical::to_canonical_bytes;

pub fn hash_bytes(input: &[u8]) -> String {
    blake3::hash(input).to_hex().to_string()
}

pub fn hash_serializable<T: Serialize>(value: &T) -> serde_json::Result<String> {
    let bytes = to_canonical_bytes(value)?;
    Ok(hash_bytes(&bytes))
}
