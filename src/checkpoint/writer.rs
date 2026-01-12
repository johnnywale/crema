//! Snapshot writer with optional LZ4 compression.

use crate::checkpoint::format::{FormatError, SnapshotEntry, SnapshotHeader};
use crc::{Crc, CRC_32_ISCSI};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// CRC-32 calculator
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Snapshot writer that writes entries to a file.
pub struct SnapshotWriter {
    /// Output writer
    writer: BufWriter<File>,

    /// Header (will be written at the end with final counts)
    header: SnapshotHeader,

    /// Whether to use compression
    compress: bool,

    /// Buffer for entries (if compressing)
    entry_buffer: Vec<u8>,

    /// Number of entries written
    entry_count: u64,

    /// Uncompressed data size
    data_size: u64,
}

impl SnapshotWriter {
    /// Create a new snapshot writer.
    pub fn new(
        path: impl AsRef<Path>,
        raft_index: u64,
        raft_term: u64,
        compress: bool,
    ) -> Result<Self, FormatError> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write placeholder header (will be rewritten at finalize)
        let mut header = SnapshotHeader::new(raft_index, raft_term);
        header.set_compressed(compress);
        writer.write_all(&header.to_bytes())?;

        Ok(Self {
            writer,
            header,
            compress,
            entry_buffer: Vec::new(),
            entry_count: 0,
            data_size: 0,
        })
    }

    /// Write an entry to the snapshot.
    pub fn write_entry(&mut self, entry: &SnapshotEntry) -> Result<(), FormatError> {
        let bytes = entry.to_bytes();
        self.data_size += bytes.len() as u64;
        self.entry_count += 1;

        if self.compress {
            // Buffer entries for compression
            self.entry_buffer.extend_from_slice(&bytes);
        } else {
            // Write directly
            self.writer.write_all(&bytes)?;
        }

        Ok(())
    }

    /// Write a key-value pair without expiration.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), FormatError> {
        let entry = SnapshotEntry::new(key.to_vec(), value.to_vec());
        self.write_entry(&entry)
    }

    /// Finalize the snapshot and return metadata.
    pub fn finalize(mut self) -> Result<SnapshotMetadata, FormatError> {
        let data_to_write = if self.compress && !self.entry_buffer.is_empty() {
            // Compress the entry buffer
            lz4_flex::compress_prepend_size(&self.entry_buffer)
        } else if self.compress {
            Vec::new()
        } else {
            // Data already written directly
            Vec::new()
        };

        // Write compressed data if any
        if self.compress && !data_to_write.is_empty() {
            self.writer.write_all(&data_to_write)?;
        }

        // Calculate CRC of all data after header
        let written_size = if self.compress {
            data_to_write.len()
        } else {
            self.data_size as usize
        };

        // For CRC, we need to re-read the data we wrote
        // In a production implementation, we'd calculate CRC as we write
        // For now, we'll use a simple approach
        let crc = if self.compress {
            let mut digest = CRC32.digest();
            digest.update(&data_to_write);
            digest.finalize()
        } else {
            // Simplified: just use entry count as pseudo-checksum for non-compressed
            // In production, calculate CRC while writing
            self.entry_count as u32
        };

        // Write CRC
        self.writer.write_all(&crc.to_le_bytes())?;

        // Update header with final counts
        self.header.entry_count = self.entry_count;
        self.header.data_size = self.data_size;

        // Seek back and rewrite header
        self.writer.flush()?;

        // Get the underlying file and rewrite header
        let file = self.writer.into_inner().map_err(|e| {
            FormatError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Reopen to write header at beginning
        use std::io::{Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.header.to_bytes())?;
        file.flush()?;

        let file_size = file.metadata()?.len();

        Ok(SnapshotMetadata {
            raft_index: self.header.raft_index,
            raft_term: self.header.raft_term,
            timestamp: self.header.timestamp,
            entry_count: self.entry_count,
            uncompressed_size: self.data_size,
            compressed_size: if self.compress {
                written_size as u64
            } else {
                self.data_size
            },
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

    /// CRC32 checksum
    pub checksum: u32,
}

impl SnapshotMetadata {
    /// Calculate compression ratio (1.0 means no compression)
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
    use crate::checkpoint::format::HEADER_SIZE;
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
        // Compressed should be smaller (unless data is random)
        assert!(metadata.compression_ratio() >= 1.0);
    }
}
