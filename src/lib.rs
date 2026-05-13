use std::future::Future;
use std::ops::Index;
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

impl<'a, T> PagableStream for PagedStream<'a, T>
where
    T: Index<usize> + Sync,
    T::Output: Sized + Sync,
{
    type Item = T::Output;

    async fn prev(&mut self) -> Option<&Self::Item> {
        if self.index == 0 {
            return None;
        }
        self.index -= 1;
        Some(&self.source.index(self.index))
    }

    async fn next(&mut self) -> Option<&Self::Item> {
        if self.index == 0 {
            return None;
        }
        self.index += 1;
        Some(&self.source.index(self.index))
    }
}

#[cfg(test)]
mod test {
    use crate::PagableStream;

    use super::PagedStream;

    #[tokio::test]
    async fn does_it_run() {
        let test_vec = vec![1, 2, 3];

        let st = PagedStream::new(&test_vec);
        assert_eq!(st.next().await, Some(&2));
        assert_eq!(st.prev().await, Some(&1));
    }
}
