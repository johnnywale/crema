//! Snapshot reader with streaming LZ4 decompression.

use crate::checkpoint::format::{FormatError, SnapshotEntry, SnapshotHeader, HEADER_SIZE};
use crc::{Crc, CRC_32_ISCSI};
use lz4_flex::frame::FrameDecoder;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

/// CRC-32 calculator (iSCSI polynomial)
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Wrapper that calculates CRC32 while reading.
struct CrcReader<R: Read> {
    inner: R,
    digest: crc::Digest<'static, u32>,
}

impl<R: Read> CrcReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: CRC32.digest(),
        }
    }

    fn current_crc(&self) -> u32 {
        self.digest.clone().finalize()
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.digest.update(&buf[..n]);
        Ok(n)
    }
}

/// Inner reader that handles decompression transparently.
enum ReaderInner {
    /// Compressed input using LZ4 frame format.
    /// CRC verification relies on LZ4 frame's built-in content checksum.
    Compressed {
        decoder: FrameDecoder<CrcReader<BufReader<File>>>,
    },
    /// Uncompressed input with CRC tracking
    Uncompressed {
        reader: CrcReader<BufReader<File>>,
        stored_crc: u32,
    },
}

/// Snapshot reader that reads entries from a file.
///
/// Uses streaming decompression to avoid loading entire snapshot into memory.
pub struct SnapshotReader {
    /// The snapshot header
    header: SnapshotHeader,

    /// Inner reader (compressed or uncompressed)
    inner: ReaderInner,

    /// Number of entries read so far
    entries_read: u64,
}

impl SnapshotReader {
    /// Open a snapshot file for reading.
    ///
    /// This only reads the header and prepares for streaming.
    /// Entries are read on-demand via `read_entry()`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;
        let header = SnapshotHeader::from_bytes(&header_buf)?;

        // Read stored CRC from end of file
        // File layout: [header][data][crc32]
        reader.seek(SeekFrom::End(-4))?;
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        // Seek back to start of data
        reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        // Validate file size
        if file_size < HEADER_SIZE as u64 + 4 {
            return Err(FormatError::InvalidHeader("file too small".into()));
        }

        // Create inner reader based on compression
        let inner = if header.is_compressed() {
            // Wrap in CRC reader to verify compressed data
            let crc_reader = CrcReader::new(reader);

            // Wrap in LZ4 frame decoder
            let decoder = FrameDecoder::new(crc_reader);

            ReaderInner::Compressed { decoder }
        } else {
            // For uncompressed, wrap directly in CRC reader
            let crc_reader = CrcReader::new(reader);
            ReaderInner::Uncompressed {
                reader: crc_reader,
                stored_crc,
            }
        };

        Ok(Self {
            header,
            inner,
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
        self.entries_read < self.header.entry_count
    }

    /// Read the next entry from the snapshot.
    ///
    /// Returns `Ok(None)` when all entries have been read.
    /// Verifies the per-entry CRC to detect corruption.
    pub fn read_entry(&mut self) -> Result<Option<SnapshotEntry>, FormatError> {
        if !self.has_more() {
            return Ok(None);
        }

        // Buffer to accumulate data for CRC verification
        let mut entry_data = Vec::new();

        // Read key length
        let mut len_buf = [0u8; 4];
        self.read_exact(&mut len_buf)?;
        entry_data.extend_from_slice(&len_buf);
        let key_len = u32::from_le_bytes(len_buf) as usize;

        // Read key
        let mut key = vec![0u8; key_len];
        self.read_exact(&mut key)?;
        entry_data.extend_from_slice(&key);

        // Read value length
        self.read_exact(&mut len_buf)?;
        entry_data.extend_from_slice(&len_buf);
        let value_len = u32::from_le_bytes(len_buf) as usize;

        // Read value
        let mut value = vec![0u8; value_len];
        self.read_exact(&mut value)?;
        entry_data.extend_from_slice(&value);

        // Read expiration flag
        let mut flag_buf = [0u8; 1];
        self.read_exact(&mut flag_buf)?;
        entry_data.extend_from_slice(&flag_buf);
        let has_expiration = flag_buf[0] != 0;

        // Read expiration if present
        let expires_at = if has_expiration {
            let mut exp_buf = [0u8; 8];
            self.read_exact(&mut exp_buf)?;
            entry_data.extend_from_slice(&exp_buf);
            Some(u64::from_le_bytes(exp_buf))
        } else {
            None
        };

        // Read and verify entry CRC
        let mut crc_buf = [0u8; 4];
        self.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        let mut digest = CRC32.digest();
        digest.update(&entry_data);
        let computed_crc = digest.finalize();

        if stored_crc != computed_crc {
            return Err(FormatError::EntryCorrupted {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        self.entries_read += 1;

        Ok(Some(SnapshotEntry {
            key,
            value,
            expires_at,
        }))
    }

    /// Helper to read exact bytes from the inner reader.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), FormatError> {
        match &mut self.inner {
            ReaderInner::Compressed { decoder, .. } => {
                decoder.read_exact(buf)?;
            }
            ReaderInner::Uncompressed { reader, .. } => {
                reader.read_exact(buf)?;
            }
        }
        Ok(())
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

    /// Verify the CRC checksum after reading all entries.
    ///
    /// This should be called after reading all entries to verify data integrity.
    /// For compressed snapshots, LZ4 frame format has built-in content checksums.
    /// For uncompressed snapshots, this verifies the raw data CRC.
    pub fn verify_checksum(&mut self) -> Result<(), FormatError> {
        // Read any remaining data to complete CRC calculation
        let mut drain = [0u8; 4096];
        loop {
            let n = match &mut self.inner {
                ReaderInner::Compressed { decoder } => decoder.read(&mut drain)?,
                ReaderInner::Uncompressed { reader, .. } => reader.read(&mut drain)?,
            };
            if n == 0 {
                break;
            }
        }

        // Get computed CRC and compare (only for uncompressed)
        // Compressed files rely on LZ4 frame's built-in content checksum
        if let ReaderInner::Uncompressed { reader, stored_crc } = &self.inner {
            let computed = reader.current_crc();
            if computed != *stored_crc {
                return Err(FormatError::ChecksumMismatch {
                    expected: *stored_crc,
                    actual: computed,
                });
            }
        }

        Ok(())
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

        // Verify content
        assert_eq!(entries[0].key, b"key0000");
        assert_eq!(entries[99].key, b"key0099");
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

    #[test]
    fn test_streaming_read() {
        // Test that we can read entries one at a time without loading all
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.lz4");
        create_test_snapshot(&path, true, 1000);

        let mut reader = SnapshotReader::open(&path).unwrap();

        // Read entries one by one
        let mut count = 0;
        while let Some(entry) = reader.read_entry().unwrap() {
            let expected_key = format!("key{:04}", count);
            assert_eq!(entry.key, expected_key.as_bytes());
            count += 1;
        }

        assert_eq!(count, 1000);
    }

    #[test]
    fn test_empty_snapshot() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.dat");

        let writer = SnapshotWriter::new(&path, 50, 3, false).unwrap();
        writer.finalize().unwrap();

        let mut reader = SnapshotReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 0);
        assert!(!reader.has_more());

        let entries = reader.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_roundtrip_with_expiration() {
        use crate::checkpoint::format::SnapshotEntry;
        use std::time::{Duration, SystemTime};

        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.dat");

        // Write entry with expiration
        let mut writer = SnapshotWriter::new(&path, 1, 1, false).unwrap();
        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        let entry = SnapshotEntry::with_expiration(b"key".to_vec(), b"value".to_vec(), expires_at);
        writer.write_entry(&entry).unwrap();
        writer.finalize().unwrap();

        // Read back
        let mut reader = SnapshotReader::open(&path).unwrap();
        let read_entry = reader.read_entry().unwrap().unwrap();

        assert_eq!(read_entry.key, b"key");
        assert_eq!(read_entry.value, b"value");
        assert!(read_entry.expires_at.is_some());
        assert!(!read_entry.is_expired());
    }
}
