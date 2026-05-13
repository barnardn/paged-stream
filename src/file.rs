use crate::PageableStream;
use tokio::fs::File;

#[allow(dead_code)]
pub struct PagedFileStream<'a> {
    index: usize,
    blk_size: usize,
    cache: Vec<Vec<u8>>,
    source: &'a File,
}

impl<'a> PagedFileStream<'a> {
    pub fn new(source: &'a File) -> Self {
        PagedFileStream {
            index: 0,
            blk_size: 1024,
            cache: Vec::with_capacity(16), // 16 pages of cache for now
            source,
        }
    }

    pub fn set_blk_size(&mut self, blk_size: usize) {
        self.blk_size = blk_size;
    }
}

impl<'a> PageableStream for PagedFileStream<'a> {
    type Item = &'a [u8];

    async fn prev(&mut self) -> Option<&Self::Item> {
        unimplemented!();
    }

    async fn next(&mut self) -> Option<&Self::Item> {
        unimplemented!();
    }
}
