use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    pub root: PathBuf,
}
