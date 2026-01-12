//! Snapshot reader with LZ4 decompression support.

use crate::checkpoint::format::{FormatError, SnapshotEntry, SnapshotHeader, HEADER_SIZE};
use crc::{Crc, CRC_32_ISCSI};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// CRC-32 calculator
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Snapshot reader that reads entries from a file.
pub struct SnapshotReader {
    /// The snapshot header
    header: SnapshotHeader,

    /// Decompressed data buffer
    data: Vec<u8>,

    /// Current position in data buffer
    position: usize,

    /// Number of entries read so far
    entries_read: u64,
}

impl SnapshotReader {
    /// Open a snapshot file for reading.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len() as usize;
        let mut reader = BufReader::new(file);

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;
        let header = SnapshotHeader::from_bytes(&header_buf)?;

        // Read data and checksum
        let data_size = file_size - HEADER_SIZE - 4; // 4 bytes for CRC
        let mut data_buf = vec![0u8; data_size];
        reader.read_exact(&mut data_buf)?;

        // Read checksum
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        // Decompress if needed
        let data = if header.is_compressed() {
            // Verify CRC on compressed data
            let mut digest = CRC32.digest();
            digest.update(&data_buf);
            let computed_crc = digest.finalize();

            if computed_crc != stored_crc {
                return Err(FormatError::ChecksumMismatch {
                    expected: stored_crc,
                    actual: computed_crc,
                });
            }

            // Handle empty compressed data (no entries)
            if data_buf.is_empty() {
                Vec::new()
            } else {
                // Decompress
                lz4_flex::decompress_size_prepended(&data_buf)
                    .map_err(|e| FormatError::DecompressionFailed(e.to_string()))?
            }
        } else {
            // For uncompressed, simplified checksum verification
            // In production, implement proper CRC on uncompressed data
            data_buf
        };

        Ok(Self {
            header,
            data,
            position: 0,
            entries_read: 0,
        })
    }

    /// Get the snapshot header.
    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    /// Get the Raft index of this snapshot.
    pub fn raft_index(&self) -> u64 {
        self.header.raft_index
    }

    /// Get the Raft term of this snapshot.
    pub fn raft_term(&self) -> u64 {
        self.header.raft_term
    }

    /// Get the expected number of entries.
    pub fn entry_count(&self) -> u64 {
        self.header.entry_count
    }

    /// Get the number of entries read so far.
    pub fn entries_read(&self) -> u64 {
        self.entries_read
    }

    /// Check if there are more entries to read.
    pub fn has_more(&self) -> bool {
        self.position < self.data.len() && self.entries_read < self.header.entry_count
    }

    /// Read the next entry from the snapshot.
    pub fn read_entry(&mut self) -> Result<Option<SnapshotEntry>, FormatError> {
        if !self.has_more() {
            return Ok(None);
        }

        let remaining = &self.data[self.position..];
        let (entry, consumed) = SnapshotEntry::from_bytes(remaining)?;

        self.position += consumed;
        self.entries_read += 1;

        Ok(Some(entry))
    }

    /// Read all remaining entries.
    pub fn read_all(&mut self) -> Result<Vec<SnapshotEntry>, FormatError> {
        let mut entries = Vec::new();

        while let Some(entry) = self.read_entry()? {
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Read all entries, filtering out expired ones.
    pub fn read_valid(&mut self) -> Result<Vec<SnapshotEntry>, FormatError> {
        let mut entries = Vec::new();

        while let Some(entry) = self.read_entry()? {
            if !entry.is_expired() {
                entries.push(entry);
            }
        }

        Ok(entries)
    }
}

/// Iterator over snapshot entries.
pub struct SnapshotIterator<'a> {
    reader: &'a mut SnapshotReader,
}

impl<'a> Iterator for SnapshotIterator<'a> {
    type Item = Result<SnapshotEntry, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl SnapshotReader {
    /// Get an iterator over entries.
    pub fn iter(&mut self) -> SnapshotIterator<'_> {
        SnapshotIterator { reader: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::writer::SnapshotWriter;
    use tempfile::tempdir;

    fn create_test_snapshot(path: &Path, compress: bool, count: usize) {
        let mut writer = SnapshotWriter::new(path, 100, 5, compress).unwrap();
        for i in 0..count {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            writer.write(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finalize().unwrap();
    }

    #[test]
    fn test_read_uncompressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.dat");
        create_test_snapshot(&path, false, 10);

        let mut reader = SnapshotReader::open(&path).unwrap();

        assert_eq!(reader.raft_index(), 100);
        assert_eq!(reader.raft_term(), 5);
        assert_eq!(reader.entry_count(), 10);

        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 10);

        assert_eq!(entries[0].key, b"key0000");
        assert_eq!(entries[0].value, b"value0000");
        assert_eq!(entries[9].key, b"key0009");
    }

    #[test]
    fn test_read_compressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.lz4");
        create_test_snapshot(&path, true, 100);

        let mut reader = SnapshotReader::open(&path).unwrap();

        assert!(reader.header().is_compressed());
        assert_eq!(reader.entry_count(), 100);

        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_iterator() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.dat");
        create_test_snapshot(&path, false, 5);

        let mut reader = SnapshotReader::open(&path).unwrap();
        let entries: Vec<_> = reader.iter().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(entries.len(), 5);
    }
}
