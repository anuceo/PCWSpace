use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("resource not found: {0}")]
    NotFound(String),
}
