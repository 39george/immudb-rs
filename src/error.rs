#[derive(thiserror::Error)]
pub enum Error {
    #[error("unexpected error: {0}")]
    Unexpected(#[from] anyhow::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] tonic::Status),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

crate::impl_debug!(Error);
