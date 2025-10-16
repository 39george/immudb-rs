use http::uri::InvalidUri;

#[derive(thiserror::Error)]
pub enum Error {
    #[error("invalid uri: {0}")]
    InvalidUri(#[from] InvalidUri),
    #[error("unexpected error: {0}")]
    Unexpected(String),
    #[error("protocol: {0}")]
    Protocol(#[from] tonic::Status),
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("decode: {0}")]
    JsonDecode(#[from] serde_json::Error),
}

crate::impl_debug!(Error);
