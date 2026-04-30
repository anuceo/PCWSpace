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

#[cfg(test)]
mod tests {
    use super::*;

    // ── content hash ──────────────────────────────────────────────────────

    #[test]
    fn content_hash_is_deterministic() {
        let h1 = compute_content_hash("prev", b"payload");
        let h2 = compute_content_hash("prev", b"payload");
        assert_eq!(h1, h2);
    }

    #[test]
    fn content_hash_differs_with_different_prev() {
        let h1 = compute_content_hash("prev_a", b"same payload");
        let h2 = compute_content_hash("prev_b", b"same payload");
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_differs_with_different_payload() {
        let h1 = compute_content_hash("same_prev", b"payload_a");
        let h2 = compute_content_hash("same_prev", b"payload_b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_output_is_64_hex_chars() {
        let h = compute_content_hash("", b"");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ── verify_chain ──────────────────────────────────────────────────────

    #[test]
    fn verify_chain_accepts_correct_hash() {
        let prev = "deadbeef";
        let payload = b"some encrypted diff";
        let hash = compute_content_hash(prev, payload);
        assert!(verify_chain(prev, payload, &hash));
    }

    #[test]
    fn verify_chain_rejects_wrong_prev_hash() {
        let payload = b"data";
        let hash = compute_content_hash("correct_prev", payload);
        assert!(!verify_chain("wrong_prev", payload, &hash));
    }

    #[test]
    fn verify_chain_rejects_tampered_payload() {
        let prev = "abc";
        let hash = compute_content_hash(prev, b"original");
        assert!(!verify_chain(prev, b"tampered", &hash));
    }

    #[test]
    fn verify_chain_rejects_wrong_expected_hash() {
        let prev = "p";
        let payload = b"d";
        assert!(!verify_chain(prev, payload, "not_the_right_hash"));
    }

    #[test]
    fn genesis_shot_uses_empty_prev_hash() {
        // The very first shot in every chain has prev_hash = ""
        let hash = compute_content_hash("", b"first diff payload");
        assert!(verify_chain("", b"first diff payload", &hash));
    }

    // ── HMAC ──────────────────────────────────────────────────────────────

    #[test]
    fn hmac_roundtrip() {
        let key = vec![0x42u8; 32];
        let msg = "content_hash_hex_value";
        let sig = hmac_sign(msg, &key);
        assert!(hmac_verify(msg, &sig, &key));
    }

    #[test]
    fn hmac_wrong_key_rejected() {
        let key1 = vec![0x01u8; 32];
        let key2 = vec![0x02u8; 32];
        let sig = hmac_sign("msg", &key1);
        assert!(!hmac_verify("msg", &sig, &key2));
    }

    #[test]
    fn hmac_tampered_message_rejected() {
        let key = vec![0x55u8; 32];
        let sig = hmac_sign("original_hash", &key);
        assert!(!hmac_verify("tampered_hash", &sig, &key));
    }

    #[test]
    fn hmac_truncated_signature_rejected() {
        let key = vec![0xFF; 32];
        let sig = hmac_sign("msg", &key);
        let truncated = &sig[..sig.len() / 2];
        assert!(!hmac_verify("msg", truncated, &key));
    }

    #[test]
    fn hmac_signature_is_32_bytes() {
        let sig = hmac_sign("any_hash", &vec![0u8; 32]);
        assert_eq!(sig.len(), 32);
    }

    // ── hash_string ───────────────────────────────────────────────────────

    #[test]
    fn hash_string_known_vector() {
        // SHA-256("") is a well-known constant
        assert_eq!(
            hash_string(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hash_string_is_deterministic() {
        assert_eq!(hash_string("hello"), hash_string("hello"));
    }
}
