//! Aligned buffer pool implementation

use std::sync::Mutex;

use crate::utility::AlignedBuffer;

/// A buffer checked out from the pool.
/// Returns to the pool on drop.
pub struct PooledBuffer {
    buffer: Option<AlignedBuffer>,
    pool: Option<std::sync::Weak<AlignedBufferPoolInner>>,
}

impl PooledBuffer {
    /// Create a standalone buffer not backed by a pool
    pub fn standalone(size: usize, alignment: usize) -> Option<Self> {
        AlignedBuffer::zeroed(alignment, size).map(|buf| Self {
            buffer: Some(buf),
            pool: None,
        })
    }

    /// Get a pointer to the buffer data
    pub fn as_ptr(&self) -> *const u8 {
        self.buffer
            .as_ref()
            .map_or(std::ptr::null(), |b| b.as_ptr())
    }

    /// Get a mutable pointer to the buffer data
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buffer
            .as_mut()
            .map_or(std::ptr::null_mut(), |b| b.as_mut_ptr())
    }

    /// Get a slice of the buffer
    pub fn as_slice(&self) -> &[u8] {
        self.buffer.as_ref().map_or(&[], |b| b.as_slice())
    }

    /// Get a mutable slice of the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut().map_or(&mut [], |b| b.as_mut_slice())
    }

    /// Get the buffer size
    pub fn len(&self) -> usize {
        self.buffer.as_ref().map_or(0, |b| b.size())
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(weak) = self.pool.take() {
            if let Some(pool) = weak.upgrade() {
                if let Some(buffer) = self.buffer.take() {
                    pool.return_buffer(buffer);
                }
            }
        }
    }
}

struct AlignedBufferPoolInner {
    buffers: Mutex<Vec<AlignedBuffer>>,
    buffer_size: usize,
    alignment: usize,
    max_pooled: usize,
}

impl AlignedBufferPoolInner {
    fn return_buffer(&self, buffer: AlignedBuffer) {
        let mut pool = self.buffers.lock().unwrap();
        if pool.len() < self.max_pooled {
            pool.push(buffer);
        }
        // Otherwise just drop it
    }
}

/// Pool of sector-aligned memory buffers for I/O operations.
///
/// Based on C++ FASTER's NativeSectorAlignedBufferPool.
/// Pre-allocates buffers and reuses them to avoid repeated allocation overhead.
pub struct AlignedBufferPool {
    inner: std::sync::Arc<AlignedBufferPoolInner>,
}

impl AlignedBufferPool {
    /// Create a new buffer pool
    ///
    /// # Arguments
    /// * `buffer_size` - Size of each buffer in bytes
    /// * `alignment` - Required alignment (typically sector size, e.g., 512 or 4096)
    /// * `initial_count` - Number of buffers to pre-allocate
    /// * `max_pooled` - Maximum number of buffers to keep in the pool
    pub fn new(
        buffer_size: usize,
        alignment: usize,
        initial_count: usize,
        max_pooled: usize,
    ) -> Self {
        let mut buffers = Vec::with_capacity(initial_count);
        for _ in 0..initial_count {
            if let Some(buf) = AlignedBuffer::zeroed(alignment, buffer_size) {
                buffers.push(buf);
            }
        }

        Self {
            inner: std::sync::Arc::new(AlignedBufferPoolInner {
                buffers: Mutex::new(buffers),
                buffer_size,
                alignment,
                max_pooled,
            }),
        }
    }

    /// Get a buffer from the pool, or allocate a new one if empty
    pub fn get(&self) -> Option<PooledBuffer> {
        let buffer = {
            let mut pool = self.inner.buffers.lock().unwrap();
            pool.pop()
        };

        let buffer = match buffer {
            Some(buf) => buf,
            None => AlignedBuffer::zeroed(self.inner.alignment, self.inner.buffer_size)?,
        };

        Some(PooledBuffer {
            buffer: Some(buffer),
            pool: Some(std::sync::Arc::downgrade(&self.inner)),
        })
    }

    /// Get the number of currently pooled buffers
    pub fn available(&self) -> usize {
        self.inner.buffers.lock().unwrap().len()
    }

    /// Get the configured buffer size
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size
    }

    /// Get the configured alignment
    pub fn alignment(&self) -> usize {
        self.inner.alignment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let pool = AlignedBufferPool::new(4096, 512, 4, 8);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.buffer_size(), 4096);
        assert_eq!(pool.alignment(), 512);
    }

    #[test]
    fn test_pool_get_and_return() {
        let pool = AlignedBufferPool::new(4096, 512, 2, 4);
        assert_eq!(pool.available(), 2);

        {
            let buf = pool.get().unwrap();
            assert_eq!(buf.len(), 4096);
            assert_eq!(pool.available(), 1);
        }
        // Buffer returned on drop
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_pool_exhaustion_and_allocation() {
        let pool = AlignedBufferPool::new(4096, 512, 1, 4);
        assert_eq!(pool.available(), 1);

        let _buf1 = pool.get().unwrap();
        assert_eq!(pool.available(), 0);

        // Should still be able to get a buffer (allocates new one)
        let _buf2 = pool.get().unwrap();
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn test_pool_max_limit() {
        let pool = AlignedBufferPool::new(4096, 512, 0, 2);

        let bufs: Vec<_> = (0..5).filter_map(|_| pool.get()).collect();
        assert_eq!(pool.available(), 0);

        // Drop all buffers
        drop(bufs);
        // Only max_pooled (2) should be retained
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_standalone_buffer() {
        let buf = PooledBuffer::standalone(1024, 64).unwrap();
        assert_eq!(buf.len(), 1024);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_buffer_read_write() {
        let pool = AlignedBufferPool::new(4096, 512, 1, 4);
        let mut buf = pool.get().unwrap();

        let data = buf.as_mut_slice();
        data[0] = 42;
        data[1] = 43;

        let data = buf.as_slice();
        assert_eq!(data[0], 42);
        assert_eq!(data[1], 43);
    }
}
