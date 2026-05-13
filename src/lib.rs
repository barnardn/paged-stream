use std::future::Future;

pub mod vec;

pub trait PageableStream {
    type Item;
    fn prev(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
    fn next(&mut self) -> impl Future<Output = Option<&Self::Item>> + Send;
}

pub struct PagedStream<'a, T> {
    index: usize,
    source: &'a T,
}

impl<'a, T> PagedStream<'a, T> {
    pub fn new(source: &'a T) -> Self {
        PagedStream { index: 0, source }
    }
}
