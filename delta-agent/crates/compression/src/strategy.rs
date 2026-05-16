#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionTier {
    Fast,
    Balanced,
    Max,
}

pub fn choose_tier(payload_size: usize) -> CompressionTier {
    match payload_size {
        0..=1024 => CompressionTier::Fast,
        1025..=1_048_576 => CompressionTier::Balanced,
        _ => CompressionTier::Max,
    }
}
