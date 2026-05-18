#[derive(Debug, Clone, Default)]
pub struct HashChain {
    pub tips: Vec<String>,
}

impl HashChain {
    pub fn append(&mut self, payload: &[u8]) -> String {
        let prev = self.tips.last().map(String::as_str).unwrap_or("");
        let mut hasher = blake3::Hasher::new();
        hasher.update(prev.as_bytes());
        hasher.update(payload);
        let digest = hasher.finalize().to_hex().to_string();
        self.tips.push(digest.clone());
        digest
    }
}
