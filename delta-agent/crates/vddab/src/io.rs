use std::path::Path;

use tokio::fs;

pub async fn read_bytes(path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    fs::read(path).await
}

pub async fn write_bytes(path: impl AsRef<Path>, payload: &[u8]) -> std::io::Result<()> {
    fs::write(path, payload).await
}
