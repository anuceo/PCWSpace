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
