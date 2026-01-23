//! Raft snapshot data serialization for InstallSnapshot RPC.
//!
//! This module provides serialization/deserialization of cache state for the
//! Raft Snapshot.data field. When a leader needs to send a snapshot to a
//! lagging follower via InstallSnapshot RPC, the cache entries are serialized
//! using this format.
//!
//! # Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │ MAGIC: [u8; 4] = "RSND" (Raft SNapshot Data)    │
//! ├─────────────────────────────────────────────────┤
//! │ VERSION: u32 = 1                                │
//! ├─────────────────────────────────────────────────┤
//! │ ENTRY_COUNT: u64                                │
//! ├─────────────────────────────────────────────────┤
//! │ DATA_SIZE: u64 (uncompressed)                   │
//! ├─────────────────────────────────────────────────┤
//! │ ENTRIES (LZ4 compressed block)                  │
//! │   Entry: key_len(4) + key + val_len(4) + value  │
//! ├─────────────────────────────────────────────────┤
//! │ CRC32: u32                                      │
//! └─────────────────────────────────────────────────┘
//! ```

use bytes::Bytes;
use crc::{Crc, CRC_32_ISCSI};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use std::io::{self, Cursor, Read, Write};
use thiserror::Error;

/// CRC-32 calculator (iSCSI polynomial)
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Magic number for Raft snapshot data: "RSND" (Raft SNapshot Data)
const MAGIC: [u8; 4] = [b'R', b'S', b'N', b'D'];

/// Format version
/// Version 2 adds expires_at field for TTL support
const VERSION: u32 = 2;

/// Header size: magic(4) + version(4) + entry_count(8) + data_size(8) = 24 bytes
const HEADER_SIZE: usize = 24;

/// Maximum snapshot data size (256MB) - larger snapshots should use chunked transfer
const MAX_SNAPSHOT_SIZE: usize = 256 * 1024 * 1024;

/// Errors during snapshot data serialization/deserialization
#[derive(Debug, Error)]
pub enum SnapshotDataError {
    #[error("invalid magic number")]
    InvalidMagic,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("snapshot too large: {size} bytes (max: {max})")]
    TooLarge { size: usize, max: usize },

    #[error("checksum mismatch: expected {expected:08x}, got {actual:08x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("compression failed: {0}")]
    CompressionFailed(String),

    #[error("truncated data: expected {expected} bytes, got {actual}")]
    TruncatedData { expected: usize, actual: usize },

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Serialize cache entries to Raft snapshot data.
///
/// The entries are compressed using LZ4 streaming compression for efficient
/// memory usage - we never hold both uncompressed and compressed data in memory.
///
/// # Arguments
/// * `entries` - Iterator of (key, value, expires_at_ms) tuples to serialize.
///               expires_at_ms is the absolute expiration time in milliseconds since Unix epoch,
///               or None for entries without TTL.
///
/// # Returns
/// Serialized bytes suitable for Raft Snapshot.data field
pub fn serialize_snapshot_data<'a>(
    entries: impl Iterator<Item = (&'a [u8], &'a [u8], Option<u64>)>,
) -> Result<Vec<u8>, SnapshotDataError> {
    // Pre-allocate result buffer with header space
    // We'll write header placeholder first, then compress directly into buffer
    let mut result = Vec::with_capacity(64 * 1024); // Start with 64KB

    // Write header placeholder (will be updated at the end)
    result.write_all(&MAGIC)?;
    result.write_all(&VERSION.to_le_bytes())?;
    result.write_all(&0u64.to_le_bytes())?; // entry_count placeholder
    result.write_all(&0u64.to_le_bytes())?; // data_size placeholder

    // header_end marks where compressed data begins (used for debugging)

    // Stream entries directly through LZ4 encoder into result buffer
    let mut entry_count: u64 = 0;
    let mut data_size: u64 = 0;

    {
        let mut encoder = FrameEncoder::new(&mut result);

        for (key, value, expires_at_ms) in entries {
            // Write key: length + data
            let key_len_bytes = (key.len() as u32).to_le_bytes();
            encoder.write_all(&key_len_bytes)?;
            encoder.write_all(key)?;

            // Write value: length + data
            let value_len_bytes = (value.len() as u32).to_le_bytes();
            encoder.write_all(&value_len_bytes)?;
            encoder.write_all(value)?;

            // Write expiration time (0 = no expiration)
            let expires_at = expires_at_ms.unwrap_or(0);
            encoder.write_all(&expires_at.to_le_bytes())?;

            // Track uncompressed size
            data_size += (4 + key.len() + 4 + value.len() + 8) as u64;
            entry_count += 1;
        }

        // Finish compression - writes frame footer
        encoder
            .finish()
            .map_err(|e| SnapshotDataError::CompressionFailed(e.to_string()))?;
    }

    // Check size limit
    let total_size = result.len() + 4; // +4 for CRC
    if total_size > MAX_SNAPSHOT_SIZE {
        return Err(SnapshotDataError::TooLarge {
            size: total_size,
            max: MAX_SNAPSHOT_SIZE,
        });
    }

    // Update header with actual counts
    result[8..16].copy_from_slice(&entry_count.to_le_bytes());
    result[16..24].copy_from_slice(&data_size.to_le_bytes());

    // Calculate and write CRC over header + compressed data
    let mut digest = CRC32.digest();
    digest.update(&result);
    let crc = digest.finalize();
    result.write_all(&crc.to_le_bytes())?;

    Ok(result)
}

/// Entry from deserialized snapshot with optional expiration.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Bytes,
    pub value: Bytes,
    /// Expiration time in milliseconds since Unix epoch, or None for no expiration.
    pub expires_at_ms: Option<u64>,
}

impl SnapshotEntry {
    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        self.expires_at_ms.map_or(false, |expires_at| {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            expires_at <= now_ms
        })
    }
}

/// Deserialize Raft snapshot data to cache entries.
///
/// Uses streaming decompression to avoid holding both compressed and
/// uncompressed data in memory simultaneously.
///
/// # Arguments
/// * `data` - Serialized snapshot data from Raft Snapshot.data field
///
/// # Returns
/// Vector of SnapshotEntry containing key, value, and optional expiration
pub fn deserialize_snapshot_data(data: &[u8]) -> Result<Vec<SnapshotEntry>, SnapshotDataError> {
    if data.len() < HEADER_SIZE + 4 {
        return Err(SnapshotDataError::TruncatedData {
            expected: HEADER_SIZE + 4,
            actual: data.len(),
        });
    }

    // Check magic first (before CRC) for clearer error messages
    if data[0..4] != MAGIC {
        return Err(SnapshotDataError::InvalidMagic);
    }

    // Verify CRC
    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());

    let mut digest = CRC32.digest();
    digest.update(&data[..crc_offset]);
    let computed_crc = digest.finalize();

    if stored_crc != computed_crc {
        return Err(SnapshotDataError::ChecksumMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    // Parse rest of header
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    // Support both version 1 (no expiration) and version 2 (with expiration)
    if version != 1 && version != VERSION {
        return Err(SnapshotDataError::UnsupportedVersion(version));
    }

    let entry_count = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let _data_size = u64::from_le_bytes(data[16..24].try_into().unwrap());

    // Decompress entries using streaming decoder
    let compressed = &data[HEADER_SIZE..crc_offset];
    let mut decoder = FrameDecoder::new(Cursor::new(compressed));

    // Parse entries directly from decompressor
    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut len_buf = [0u8; 4];
    let mut expires_buf = [0u8; 8];

    loop {
        // Try to read key length
        match decoder.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(SnapshotDataError::DecompressionFailed(e.to_string())),
        }

        let key_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; key_len];
        decoder
            .read_exact(&mut key)
            .map_err(|e| SnapshotDataError::DecompressionFailed(e.to_string()))?;

        // Read value length
        decoder
            .read_exact(&mut len_buf)
            .map_err(|e| SnapshotDataError::DecompressionFailed(e.to_string()))?;

        let value_len = u32::from_le_bytes(len_buf) as usize;
        let mut value = vec![0u8; value_len];
        decoder
            .read_exact(&mut value)
            .map_err(|e| SnapshotDataError::DecompressionFailed(e.to_string()))?;

        // Read expiration time (version 2 only)
        let expires_at_ms = if version >= 2 {
            decoder
                .read_exact(&mut expires_buf)
                .map_err(|e| SnapshotDataError::DecompressionFailed(e.to_string()))?;
            let expires_at = u64::from_le_bytes(expires_buf);
            if expires_at == 0 {
                None
            } else {
                Some(expires_at)
            }
        } else {
            None
        };

        entries.push(SnapshotEntry {
            key: Bytes::from(key),
            value: Bytes::from(value),
            expires_at_ms,
        });
    }

    Ok(entries)
}

/// Check if data is a valid Raft snapshot (quick validation without full parse)
pub fn is_valid_snapshot_data(data: &[u8]) -> bool {
    if data.len() < HEADER_SIZE + 4 {
        return false;
    }

    // Check magic
    if data[0..4] != MAGIC {
        return false;
    }

    // Check version (support both 1 and 2)
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != 1 && version != VERSION {
        return false;
    }

    // Verify CRC
    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());

    let mut digest = CRC32.digest();
    digest.update(&data[..crc_offset]);
    let computed_crc = digest.finalize();

    stored_crc == computed_crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_empty() {
        let entries: Vec<(&[u8], &[u8], Option<u64>)> = vec![];
        let data = serialize_snapshot_data(entries.into_iter()).unwrap();

        assert!(is_valid_snapshot_data(&data));

        let parsed = deserialize_snapshot_data(&data).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_roundtrip_single_entry() {
        let entries = vec![(b"key1".as_slice(), b"value1".as_slice(), None)];
        let data = serialize_snapshot_data(entries.into_iter()).unwrap();

        assert!(is_valid_snapshot_data(&data));

        let parsed = deserialize_snapshot_data(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key.as_ref(), b"key1");
        assert_eq!(parsed[0].value.as_ref(), b"value1");
        assert!(parsed[0].expires_at_ms.is_none());
    }

    #[test]
    fn test_roundtrip_multiple_entries() {
        let entries = vec![
            (b"key1".as_slice(), b"value1".as_slice(), None),
            (b"key2".as_slice(), b"value2".as_slice(), Some(1234567890000u64)),
            (b"key3".as_slice(), b"longer value with more data".as_slice(), None),
        ];
        let data = serialize_snapshot_data(entries.into_iter()).unwrap();

        assert!(is_valid_snapshot_data(&data));

        let parsed = deserialize_snapshot_data(&data).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].key.as_ref(), b"key1");
        assert_eq!(parsed[0].value.as_ref(), b"value1");
        assert!(parsed[0].expires_at_ms.is_none());
        assert_eq!(parsed[1].key.as_ref(), b"key2");
        assert_eq!(parsed[1].value.as_ref(), b"value2");
        assert_eq!(parsed[1].expires_at_ms, Some(1234567890000u64));
        assert_eq!(parsed[2].key.as_ref(), b"key3");
        assert_eq!(parsed[2].value.as_ref(), b"longer value with more data");
    }

    #[test]
    fn test_roundtrip_large_entries() {
        // Test with larger entries to verify compression
        let large_value = vec![b'x'; 10000];
        let entries = vec![(b"key".as_slice(), large_value.as_slice(), None)];

        let data = serialize_snapshot_data(entries.into_iter()).unwrap();

        // Verify compression happened (compressed should be smaller)
        assert!(data.len() < 10000);

        let parsed = deserialize_snapshot_data(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value.len(), 10000);
    }

    #[test]
    fn test_invalid_magic() {
        let entries: Vec<(&[u8], &[u8], Option<u64>)> = vec![];
        let mut data = serialize_snapshot_data(entries.into_iter()).unwrap();
        data[0] = b'X'; // Corrupt magic

        assert!(!is_valid_snapshot_data(&data));
        // Now that we check magic first, this should return InvalidMagic
        assert!(matches!(
            deserialize_snapshot_data(&data),
            Err(SnapshotDataError::InvalidMagic)
        ));
    }

    #[test]
    fn test_checksum_mismatch() {
        let entries = vec![(b"key".as_slice(), b"value".as_slice(), None)];
        let mut data = serialize_snapshot_data(entries.into_iter()).unwrap();

        // Corrupt data (not the CRC)
        data[HEADER_SIZE] ^= 0xFF;

        assert!(!is_valid_snapshot_data(&data));
        assert!(matches!(
            deserialize_snapshot_data(&data),
            Err(SnapshotDataError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_truncated_data() {
        let result = deserialize_snapshot_data(&[0; 10]);
        assert!(matches!(
            result,
            Err(SnapshotDataError::TruncatedData { .. })
        ));
    }

    #[test]
    fn test_expiration_roundtrip() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let future_ms = now_ms + 60000; // 1 minute from now
        let past_ms = now_ms - 1000; // 1 second ago

        let entries = vec![
            (b"permanent".as_slice(), b"value1".as_slice(), None),
            (b"future_ttl".as_slice(), b"value2".as_slice(), Some(future_ms)),
            (b"expired".as_slice(), b"value3".as_slice(), Some(past_ms)),
        ];
        let data = serialize_snapshot_data(entries.into_iter()).unwrap();

        let parsed = deserialize_snapshot_data(&data).unwrap();
        assert_eq!(parsed.len(), 3);

        // Permanent entry - no expiration
        assert!(parsed[0].expires_at_ms.is_none());
        assert!(!parsed[0].is_expired());

        // Future TTL - not expired
        assert_eq!(parsed[1].expires_at_ms, Some(future_ms));
        assert!(!parsed[1].is_expired());

        // Past TTL - expired
        assert_eq!(parsed[2].expires_at_ms, Some(past_ms));
        assert!(parsed[2].is_expired());
    }
}
