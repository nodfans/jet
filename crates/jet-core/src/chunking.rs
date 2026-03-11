use fastcdc::v2020::FastCDC;
use rayon::prelude::*;

const PARALLEL_CHUNKING_THRESHOLD_BYTES: usize = 512 * 1024 * 1024;
const PARALLEL_CHUNKING_WINDOW_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub offset: usize,
    pub length: usize,
}

pub trait Chunker {
    fn chunk_bytes(&self, data: &[u8]) -> Vec<ChunkDescriptor>;
}

#[derive(Debug, Clone)]
pub struct FastCdcChunker {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

impl Default for FastCdcChunker {
    fn default() -> Self {
        Self {
            min_size: 1024 * 1024,
            avg_size: 4 * 1024 * 1024,
            max_size: 16 * 1024 * 1024,
        }
    }
}

impl Chunker for FastCdcChunker {
    fn chunk_bytes(&self, data: &[u8]) -> Vec<ChunkDescriptor> {
        if data.len() >= PARALLEL_CHUNKING_THRESHOLD_BYTES {
            return self.chunk_bytes_parallel_windows(data);
        }

        self.chunk_window(data, 0)
    }
}

impl FastCdcChunker {
    fn chunk_bytes_parallel_windows(&self, data: &[u8]) -> Vec<ChunkDescriptor> {
        let window_starts = (0..data.len())
            .step_by(PARALLEL_CHUNKING_WINDOW_BYTES)
            .collect::<Vec<_>>();

        let mut windows = window_starts
            .into_par_iter()
            .map(|start| {
                let end = (start + PARALLEL_CHUNKING_WINDOW_BYTES).min(data.len());
                self.chunk_window(&data[start..end], start)
            })
            .collect::<Vec<_>>();
        windows.sort_by_key(|chunks| {
            chunks
                .first()
                .map(|chunk| chunk.offset)
                .unwrap_or(usize::MAX)
        });

        windows.into_iter().flatten().collect()
    }

    fn chunk_window(&self, data: &[u8], base_offset: usize) -> Vec<ChunkDescriptor> {
        FastCDC::new(data, self.min_size, self.avg_size, self.max_size)
            .map(|entry| ChunkDescriptor {
                offset: base_offset + entry.offset,
                length: entry.length,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Chunker, FastCdcChunker, PARALLEL_CHUNKING_WINDOW_BYTES};

    #[test]
    fn hashes_are_stable_for_identical_input() {
        let chunker = FastCdcChunker::default();
        let data = vec![b'a'; 1024 * 1024];

        let first = chunker.chunk_bytes(&data);
        let second = chunker.chunk_bytes(&data);

        assert_eq!(first, second);
        assert!(!first.is_empty());
    }

    #[test]
    fn parallel_windows_cover_the_full_file_without_gaps() {
        let chunker = FastCdcChunker::default();
        let data = vec![b'x'; (PARALLEL_CHUNKING_WINDOW_BYTES * 2) + 1024];

        let chunks = chunker.chunk_bytes_parallel_windows(&data);

        assert!(!chunks.is_empty());
        assert_eq!(chunks.first().expect("first chunk").offset, 0);
        let total: usize = chunks.iter().map(|chunk| chunk.length).sum();
        assert_eq!(total, data.len());
        for pair in chunks.windows(2) {
            assert_eq!(pair[0].offset + pair[0].length, pair[1].offset);
        }
    }
}
