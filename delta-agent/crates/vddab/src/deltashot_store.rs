use std::path::PathBuf;

use tokio::fs;

#[derive(Debug, Clone)]
pub struct DeltaShotStore {
    pub root: PathBuf,
}

impl DeltaShotStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn relative_ops_path(branch_id: &str, deltashot_id: &str) -> PathBuf {
        PathBuf::from(branch_id)
            .join("deltashots")
            .join(format!("{deltashot_id}.ops.br"))
    }

    pub fn absolute_ops_path(&self, branch_id: &str, deltashot_id: &str) -> PathBuf {
        self.root
            .join(Self::relative_ops_path(branch_id, deltashot_id))
    }

    pub async fn write_compressed_ops(
        &self,
        branch_id: &str,
        deltashot_id: &str,
        payload: &[u8],
    ) -> std::io::Result<PathBuf> {
        let path = self.absolute_ops_path(branch_id, deltashot_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, payload).await?;
        Ok(path)
    }

    pub async fn read_compressed_ops(
        &self,
        branch_id: &str,
        deltashot_id: &str,
    ) -> std::io::Result<Vec<u8>> {
        fs::read(self.absolute_ops_path(branch_id, deltashot_id)).await
    }
}
