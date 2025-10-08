pub use client::ImmuDB;
pub use interface::Interface;
pub use protocol::model;
pub use protocol::schema;

pub mod builder;
mod client;
mod error;
mod interface;
mod protocol;

pub type Result<T> = std::result::Result<T, error::Error>;
