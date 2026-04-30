use aes_gcm::{
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
    Aes256Gcm, Key, Nonce,
};
use pcw_core::errors::{PcwError, PcwResult};

pub const KEY_LEN: usize = 32;
pub const IV_LEN: usize  = 12;
pub const TAG_LEN: usize = 16;

pub fn generate_key() -> Vec<u8> {
    let mut key = vec![0u8; KEY_LEN];
    OsRng.fill_bytes(&mut key);
    key
}

/// Encrypt plaintext → [iv(12) || ciphertext_with_tag]
/// aes-gcm appends the 16-byte auth tag to the ciphertext automatically.
pub fn encrypt(plaintext: &[u8], key: &[u8]) -> PcwResult<Vec<u8>> {
    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let mut iv = [0u8; IV_LEN];
    OsRng.fill_bytes(&mut iv);
    let nonce = Nonce::from_slice(&iv);
    // aes-gcm appends the 16-byte tag to ciphertext
    let ciphertext_with_tag = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| PcwError::EncryptionFailed(e.to_string()))?;
    // Pack: iv || ciphertext_with_tag (tag is already appended by aes-gcm)
    let mut out = Vec::with_capacity(IV_LEN + ciphertext_with_tag.len());
    out.extend_from_slice(&iv);
    out.extend_from_slice(&ciphertext_with_tag);
    Ok(out)
}

/// Decrypt [iv(12) || ciphertext_with_tag]
pub fn decrypt(packed: &[u8], key: &[u8]) -> PcwResult<Vec<u8>> {
    if packed.len() < IV_LEN + TAG_LEN {
        return Err(PcwError::EncryptionFailed("payload too short".into()));
    }
    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&packed[..IV_LEN]);
    let ciphertext_with_tag = &packed[IV_LEN..];
    cipher
        .decrypt(nonce, ciphertext_with_tag)
        .map_err(|e| PcwError::EncryptionFailed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_arbitrary_plaintext() {
        let key = generate_key();
        let pt = b"hello world, this is a test payload";
        assert_eq!(decrypt(&encrypt(pt, &key).unwrap(), &key).unwrap(), pt);
    }

    #[test]
    fn roundtrip_empty_plaintext() {
        let key = generate_key();
        let pt: &[u8] = b"";
        assert_eq!(decrypt(&encrypt(pt, &key).unwrap(), &key).unwrap(), pt);
    }

    #[test]
    fn roundtrip_large_plaintext() {
        let key = generate_key();
        let pt = vec![0xABu8; 64 * 1024]; // 64 KiB
        assert_eq!(decrypt(&encrypt(&pt, &key).unwrap(), &key).unwrap(), pt);
    }

    #[test]
    fn wrong_key_fails_auth() {
        let key1 = generate_key();
        let key2 = generate_key();
        let ct = encrypt(b"secret", &key1).unwrap();
        assert!(decrypt(&ct, &key2).is_err());
    }

    #[test]
    fn truncated_payload_rejected() {
        let key = generate_key();
        // One byte shorter than the minimum IV+TAG
        let short = vec![0u8; IV_LEN + TAG_LEN - 1];
        assert!(decrypt(&short, &key).is_err());
    }

    #[test]
    fn each_encrypt_uses_unique_iv() {
        let key = generate_key();
        let pt = b"same plaintext";
        let ct1 = encrypt(pt, &key).unwrap();
        let ct2 = encrypt(pt, &key).unwrap();
        // Distinct IVs produce distinct ciphertexts
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn bitflip_in_ciphertext_fails_auth() {
        let key = generate_key();
        let mut ct = encrypt(b"authenticated data", &key).unwrap();
        // Corrupt a byte in the ciphertext (past the IV)
        ct[IV_LEN] ^= 0xFF;
        assert!(decrypt(&ct, &key).is_err());
    }

    #[test]
    fn bitflip_in_tag_fails_auth() {
        let key = generate_key();
        let pt = b"tag integrity check";
        let mut ct = encrypt(pt, &key).unwrap();
        // Corrupt the last byte (inside the auth tag)
        let last = ct.len() - 1;
        ct[last] ^= 0x01;
        assert!(decrypt(&ct, &key).is_err());
    }

    #[test]
    fn output_length_is_iv_plus_plaintext_plus_tag() {
        let key = generate_key();
        let pt = b"fixed length test";
        let ct = encrypt(pt, &key).unwrap();
        assert_eq!(ct.len(), IV_LEN + pt.len() + TAG_LEN);
    }
}
