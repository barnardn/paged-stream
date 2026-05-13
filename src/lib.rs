use core::time;
use std::future::Future;
use tokio;

pub trait PagableStream {
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

impl<'a, T: Sync> PagableStream for PagedStream<'a, Vec<T>> {
    type Item = T;

    async fn prev(&mut self) -> Option<&Self::Item> {
        if self.index == 0 {
            return None;
        } else {
            self.index -= 1;
            Some(&self.source[self.index])
        }
    }

    async fn next(&mut self) -> Option<&Self::Item> {
        if self.index == self.source.len() {
            return None;
        } else {
            let value = &self.source[self.index];
            self.index += 1;
            Some(value)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::PagableStream;

    use super::PagedStream;

    #[tokio::test]
    async fn basics() {
        let test_vec = vec![1, 2, 3];

        let mut st = PagedStream::new(&test_vec);
        assert_eq!(st.prev().await, None);
        assert_eq!(st.next().await, Some(&1));
        assert_eq!(st.next().await, Some(&2));
        assert_eq!(st.next().await, Some(&3));
        assert_eq!(st.next().await, None);
        assert_eq!(st.prev().await, Some(&3));
        assert_eq!(st.prev().await, Some(&2));
        assert_eq!(st.prev().await, Some(&1));
        assert_eq!(st.prev().await, None);
    }
}
