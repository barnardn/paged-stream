use crate::{PageableStream, PageableStreamError};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, oneshot};

struct FileSource {
    source: File,
    has_more: bool,
}

impl FileSource {
    async fn read_page(&mut self, blk_size: usize) -> Option<Vec<u8>> {
        if !self.has_more {
            return None;
        }
        let mut buf = vec![0u8; blk_size];
        let n = self.source.read(&mut buf).await.ok()?;
        if n == 0 {
            self.has_more = false;
            return None;
        }
        buf.truncate(n);
        Some(buf)
    }
}

pub struct PagedFileStream {
    index: usize,
    blk_size: usize,
    cache: Vec<Vec<u8>>,
    prefetch: Option<oneshot::Receiver<Option<Vec<u8>>>>,
    file_source: Arc<Mutex<FileSource>>,
}

impl PagedFileStream {
    pub fn new(source: File) -> Self {
        PagedFileStream {
            index: 0,
            blk_size: 1024,
            cache: vec![],
            prefetch: None,
            file_source: Arc::new(Mutex::new(FileSource {
                source,
                has_more: true,
            })),
        }
    }

    pub async fn from_path(path: &Path) -> Result<Self, PageableStreamError> {
        if path.try_exists().is_err() {
            return Err(PageableStreamError::FileNotFound(
                path.display().to_string(),
            ));
        }
        let source = tokio::fs::File::open(path).await?;

        Ok(PagedFileStream {
            index: 0,
            blk_size: 1024,
            cache: vec![],
            prefetch: None,
            file_source: Arc::new(Mutex::new(FileSource {
                source,
                has_more: true,
            })),
        })
    }

    pub fn set_blk_size(&mut self, blk_size: usize) {
        self.blk_size = blk_size;
    }

    fn spawn_prefetch(&self) -> oneshot::Receiver<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        let arc = Arc::clone(&self.file_source);
        let blk_size = self.blk_size;
        tokio::spawn(async move {
            let mut src = arc.lock().await;
            let _ = tx.send(src.read_page(blk_size).await);
        });
        rx
    }
}

impl PageableStream for PagedFileStream {
    type Item = Vec<u8>;

    async fn prev(&mut self) -> Option<&Self::Item> {
        if self.index == 0 {
            return None;
        }
        self.index -= 1;
        self.cache.get(self.index)
    }

    async fn next(&mut self) -> Option<&Self::Item> {
        // collect completed prefetch into cache
        if let Some(rx) = self.prefetch.take() {
            if let Ok(Some(page)) = rx.await {
                self.cache.push(page);
            }
        }

        // direct fetch if still not in cache
        if self.cache.get(self.index).is_none() {
            let mut src = self.file_source.lock().await;
            match src.read_page(self.blk_size).await {
                Some(page) => {
                    drop(src);
                    self.cache.push(page);
                }
                None => return None,
            }
        }

        self.prefetch = Some(self.spawn_prefetch());

        let i = self.index;
        self.index += 1;
        self.cache.get(i)
    }
}

#[cfg(test)]
mod test {
    use super::PagedFileStream;
    use crate::PageableStream;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn basic_paging() {
        let blk_size = 1024;
        let mut test_file = NamedTempFile::new().unwrap();
        populate_test_file(&mut test_file, blk_size);
        let path = test_file.path();

        let as_file = tokio::fs::File::open(&path).await.unwrap();
        let mut page_stream = PagedFileStream::new(as_file);

        paginate_stream(&mut page_stream, blk_size).await;
        _ = test_file.close();
    }

    #[tokio::test]
    async fn custom_block_size() {
        let blk_size = 4096;

        let mut test_file = NamedTempFile::new().unwrap();
        populate_test_file(&mut test_file, blk_size);
        let path = test_file.path();

        let as_file = tokio::fs::File::open(&path).await.unwrap();
        let mut page_stream = PagedFileStream::new(as_file);
        page_stream.set_blk_size(blk_size);

        paginate_stream(&mut page_stream, blk_size).await;
        _ = test_file.close();
    }

    #[tokio::test]
    async fn from_path() {
        let blk_size = 1024;
        let mut test_file = NamedTempFile::new().unwrap();
        populate_test_file(&mut test_file, blk_size);
        let path = test_file.path();

        let mut page_stream = PagedFileStream::from_path(path)
            .await
            .expect("expect path {path.display()}");

        paginate_stream(&mut page_stream, blk_size).await;

        _ = test_file.close();
    }

    async fn paginate_stream(page_stream: &mut PagedFileStream, blk_size: usize) {
        assert_eq!(page_stream.prev().await, None);
        for i in 0u8..10 {
            let mut blk = Vec::with_capacity(blk_size);
            blk.resize(blk_size, i);
            assert_eq!(page_stream.next().await, Some(&blk));
        }
        assert_eq!(page_stream.next().await, None);
        for i in (0u8..10).rev() {
            let mut blk = Vec::with_capacity(blk_size);
            blk.resize(blk_size, i);
            assert_eq!(page_stream.prev().await, Some(&blk));
        }
    }

    fn populate_test_file(test_file: &mut NamedTempFile, blk_size: usize) {
        for i in 0u8..10 {
            let mut blk = Vec::with_capacity(blk_size);
            blk.resize(blk_size, i);
            test_file.write(&blk).expect("failed writing block {i}");
        }
    }
}
