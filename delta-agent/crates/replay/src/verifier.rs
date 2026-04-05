use hashing::blake3::hash_bytes;

pub fn verify_hash(payload: &[u8], expected_hash: &str) -> bool {
    hash_bytes(payload) == expected_hash
}
