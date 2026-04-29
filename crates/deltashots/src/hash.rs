use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};

/// SHA-256(prev_hash_bytes || payload_bytes) → hex string
pub fn compute_content_hash(prev_hash: &str, payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

pub fn verify_chain(prev_hash: &str, payload: &[u8], expected_hash: &str) -> bool {
    let computed = compute_content_hash(prev_hash, payload);
    // constant-time comparison
    use std::hint::black_box;
    black_box(computed.as_bytes()) == black_box(expected_hash.as_bytes())
}

pub fn hash_string(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn hmac_sign(content_hash: &str, signing_key: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(signing_key).expect("HMAC key invalid");
    mac.update(content_hash.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub fn hmac_verify(content_hash: &str, signature: &[u8], signing_key: &[u8]) -> bool {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(signing_key).expect("HMAC key invalid");
    mac.update(content_hash.as_bytes());
    mac.verify_slice(signature).is_ok()
}
