use std::future::Future;
use thiserror::Error;

pub mod file;
pub(crate) mod vec;

pub trait PageableStream {
    type Item;
    fn prev(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
    fn next(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
}

#[derive(Debug, Error)]
pub enum PageableStreamError {
    #[error("file not found {0}")]
    FileNotFound(String),

    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),
}
