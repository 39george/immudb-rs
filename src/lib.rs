pub use client::ImmuDB;
pub use protocol::model;
pub use protocol::schema;

mod client;
mod error;
mod interceptor;
mod protocol;

pub mod document;
mod keyval;
mod sql;

pub type Result<T> = std::result::Result<T, error::Error>;

pub fn error_chain_fmt(
    e: &impl std::error::Error,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    writeln!(f, "{}\n", e)?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }
    Ok(())
}

#[macro_export]
macro_rules! impl_debug {
    ($type:ident) => {
        use $crate::error_chain_fmt;
        impl std::fmt::Debug for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                error_chain_fmt(self, f)
            }
        }
    };
}
