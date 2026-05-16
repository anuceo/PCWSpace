#[derive(Debug, Clone, Default)]
pub struct HashChain {
    pub tips: Vec<String>,
}

impl HashChain {
    pub fn append(&mut self, payload: &[u8]) -> String {
        let digest = blake3::hash(payload).to_hex().to_string();
        self.tips.push(digest.clone());
        digest
    }
}
