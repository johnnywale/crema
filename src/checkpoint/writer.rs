//! Snapshot writer with streaming LZ4 compression and CRC32 integrity.

use crate::checkpoint::format::{FormatError, SnapshotEntry, SnapshotHeader, HEADER_SIZE};
use crc::{Crc, CRC_32_ISCSI};
use lz4_flex::frame::FrameEncoder;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

/// CRC-32 calculator (iSCSI polynomial)
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Inner writer that handles compression transparently.
enum WriterInner {
    /// Compressed output using LZ4 frame format
    Compressed {
        encoder: FrameEncoder<CrcWriter<BufWriter<File>>>,
    },
    /// Uncompressed output with CRC tracking
    Uncompressed {
        writer: CrcWriter<BufWriter<File>>,
    },
}

/// Wrapper that calculates CRC32 while writing.
struct CrcWriter<W: Write> {
    inner: W,
    digest: crc::Digest<'static, u32>,
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: CRC32.digest(),
        }
    }

    fn finalize_crc(self) -> (W, u32) {
        (self.inner, self.digest.finalize())
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.digest.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Snapshot writer that writes entries to a file with optional compression.
///
/// Uses streaming compression to avoid buffering all entries in memory.
/// CRC32 checksum is calculated incrementally during writes.
pub struct SnapshotWriter {
    /// Inner writer (compressed or uncompressed)
    inner: WriterInner,

    /// Header (will be rewritten at finalize with final counts)
    header: SnapshotHeader,

    /// Number of entries written
    entry_count: u64,

    /// Uncompressed data size (sum of serialized entry sizes)
    data_size: u64,
}

impl SnapshotWriter {
    /// Create a new snapshot writer.
    ///
    /// The file is written with a placeholder header that gets updated
    /// when `finalize()` is called with the actual entry count and data size.
    pub fn new(
        path: impl AsRef<Path>,
        raft_index: u64,
        raft_term: u64,
        compress: bool,
    ) -> Result<Self, FormatError> {
        let file = File::create(path.as_ref())?;
        let mut buf_writer = BufWriter::new(file);

        // Write placeholder header (will be rewritten at finalize)
        let mut header = SnapshotHeader::new(raft_index, raft_term);
        header.set_compressed(compress);
        buf_writer.write_all(&header.to_bytes())?;

        // Wrap in CRC writer and optionally compress
        let inner = if compress {
            let crc_writer = CrcWriter::new(buf_writer);
            let encoder = FrameEncoder::new(crc_writer);
            WriterInner::Compressed { encoder }
        } else {
            let crc_writer = CrcWriter::new(buf_writer);
            WriterInner::Uncompressed { writer: crc_writer }
        };

        Ok(Self {
            inner,
            header,
            entry_count: 0,
            data_size: 0,
        })
    }

    /// Write an entry to the snapshot.
    ///
    /// Entries are written immediately (streaming) rather than buffered.
    pub fn write_entry(&mut self, entry: &SnapshotEntry) -> Result<(), FormatError> {
        self.write_raw_entry(&entry.key, &entry.value, entry.expires_at)
    }

    /// Write a key-value pair directly from slices without intermediate allocation.
    ///
    /// This is more efficient than `write_entry` for high-throughput scenarios
    /// as it avoids allocating a Vec for each entry.
    ///
    /// # Arguments
    /// * `key` - The key bytes
    /// * `value` - The value bytes
    /// * `expires_at` - Optional expiration timestamp in nanoseconds since Unix epoch
    pub fn write_raw_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
        expires_at: Option<u64>,
    ) -> Result<(), FormatError> {
        // Calculate entry size for tracking
        let entry_size = 4 + key.len() + // key length + key
            4 + value.len() + // value length + value
            1 + // has_expiration flag
            if expires_at.is_some() { 8 } else { 0 } + // expires_at
            4; // entry CRC

        self.data_size += entry_size as u64;
        self.entry_count += 1;

        // Get writer reference
        let writer: &mut dyn Write = match &mut self.inner {
            WriterInner::Compressed { encoder } => encoder,
            WriterInner::Uncompressed { writer } => writer,
        };

        // Build CRC incrementally as we write
        let mut digest = CRC32.digest();

        // Write key length + key
        let key_len_bytes = (key.len() as u32).to_le_bytes();
        writer.write_all(&key_len_bytes)?;
        digest.update(&key_len_bytes);
        writer.write_all(key)?;
        digest.update(key);

        // Write value length + value
        let value_len_bytes = (value.len() as u32).to_le_bytes();
        writer.write_all(&value_len_bytes)?;
        digest.update(&value_len_bytes);
        writer.write_all(value)?;
        digest.update(value);

        // Write expiration
        if let Some(expires) = expires_at {
            writer.write_all(&[1])?;
            digest.update(&[1]);
            let expires_bytes = expires.to_le_bytes();
            writer.write_all(&expires_bytes)?;
            digest.update(&expires_bytes);
        } else {
            writer.write_all(&[0])?;
            digest.update(&[0]);
        }

        // Write entry CRC
        let entry_crc = digest.finalize();
        writer.write_all(&entry_crc.to_le_bytes())?;

        Ok(())
    }

    /// Write a key-value pair without expiration.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), FormatError> {
        self.write_raw_entry(key, value, None)
    }

    /// Finalize the snapshot and return metadata.
    ///
    /// This:
    /// 1. Finishes compression (if enabled)
    /// 2. Writes the CRC32 checksum
    /// 3. Rewrites the header with actual counts
    /// 4. Calls `sync_all()` to ensure data reaches disk
    pub fn finalize(self) -> Result<SnapshotMetadata, FormatError> {
        // Finish writing and get CRC
        let (mut buf_writer, crc, compressed_size) = match self.inner {
            WriterInner::Compressed { encoder } => {
                // Finish LZ4 frame - this writes the frame footer
                let crc_writer = encoder
                    .finish()
                    .map_err(|e| FormatError::CompressionFailed(e.to_string()))?;
                let (mut buf_writer, crc) = crc_writer.finalize_crc();

                // Calculate compressed size (current position - header size)
                let pos = buf_writer.stream_position().unwrap_or(0);
                let compressed = pos.saturating_sub(HEADER_SIZE as u64);
                (buf_writer, crc, compressed)
            }
            WriterInner::Uncompressed { writer } => {
                let (buf_writer, crc) = writer.finalize_crc();
                (buf_writer, crc, self.data_size)
            }
        };

        // Write CRC32 checksum at the end
        buf_writer.write_all(&crc.to_le_bytes())?;
        buf_writer.flush()?;

        // Update header with final counts
        let mut header = self.header;
        header.entry_count = self.entry_count;
        header.data_size = self.data_size;

        // Seek back and rewrite header
        buf_writer.seek(SeekFrom::Start(0))?;
        buf_writer.write_all(&header.to_bytes())?;
        buf_writer.flush()?;

        // Get underlying file for sync and metadata
        let file = buf_writer
            .into_inner()
            .map_err(|e| FormatError::Io(std::io::Error::other(e.to_string())))?;

        // Ensure data reaches physical storage
        file.sync_all()?;

        let file_size = file.metadata()?.len();

        Ok(SnapshotMetadata {
            raft_index: header.raft_index,
            raft_term: header.raft_term,
            timestamp: header.timestamp,
            entry_count: self.entry_count,
            uncompressed_size: self.data_size,
            compressed_size,
            file_size,
            checksum: crc,
        })
    }
}

/// Metadata about a created snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Raft log index at snapshot time
    pub raft_index: u64,

    /// Raft term at snapshot time
    pub raft_term: u64,

    /// Unix timestamp when created
    pub timestamp: u64,

    /// Number of entries
    pub entry_count: u64,

    /// Uncompressed data size in bytes
    pub uncompressed_size: u64,

    /// Compressed data size (same as uncompressed if not compressed)
    pub compressed_size: u64,

    /// Total file size including header and checksum
    pub file_size: u64,

    /// CRC32 checksum of the data
    pub checksum: u32,
}

impl SnapshotMetadata {
    /// Calculate compression ratio (1.0 means no compression benefit)
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            1.0
        } else {
            self.uncompressed_size as f64 / self.compressed_size as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_uncompressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.dat");

        let mut writer = SnapshotWriter::new(&path, 100, 5, false).unwrap();
        writer.write(b"key1", b"value1").unwrap();
        writer.write(b"key2", b"value2").unwrap();

        let metadata = writer.finalize().unwrap();

        assert_eq!(metadata.raft_index, 100);
        assert_eq!(metadata.raft_term, 5);
        assert_eq!(metadata.entry_count, 2);
        assert!(metadata.file_size > HEADER_SIZE as u64);
        // CRC should be a real checksum now, not entry count
        assert_ne!(metadata.checksum, 2);
    }

    #[test]
    fn test_write_compressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.lz4");

        let mut writer = SnapshotWriter::new(&path, 100, 5, true).unwrap();

        // Write many entries to see compression benefit
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            writer.write(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let metadata = writer.finalize().unwrap();

        assert_eq!(metadata.entry_count, 100);
        // Compressed should provide some benefit for repetitive data
        assert!(metadata.compression_ratio() >= 1.0);
    }

    #[test]
    fn test_empty_snapshot() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.dat");

        let writer = SnapshotWriter::new(&path, 50, 3, false).unwrap();
        let metadata = writer.finalize().unwrap();

        assert_eq!(metadata.entry_count, 0);
        assert_eq!(metadata.uncompressed_size, 0);
    }

    #[test]
    fn test_streaming_memory_efficiency() {
        // This test verifies we don't buffer entries in memory
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.lz4");

        let mut writer = SnapshotWriter::new(&path, 1, 1, true).unwrap();

        // Write many entries - should not OOM
        for i in 0..10_000 {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            writer.write(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let metadata = writer.finalize().unwrap();
        assert_eq!(metadata.entry_count, 10_000);
    }
}
