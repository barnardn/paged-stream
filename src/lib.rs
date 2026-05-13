use std::future::Future;

pub mod vec;

pub trait PageableStream {
    type Item;
    fn prev(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
    fn next(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
}
