//! Checkpoint file format definitions.
//!
//! # File Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │ MAGIC_NUMBER: [u8; 4] = "MCRS" (Moka Cache RS)  │
//! ├─────────────────────────────────────────────────┤
//! │ VERSION: u32 = 1                                │
//! ├─────────────────────────────────────────────────┤
//! │ FLAGS: u32                                      │
//! │   bit 0: compressed (LZ4)                       │
//! │   bits 1-31: reserved                           │
//! ├─────────────────────────────────────────────────┤
//! │ RAFT_INDEX: u64                                 │
//! ├─────────────────────────────────────────────────┤
//! │ RAFT_TERM: u64                                  │
//! ├─────────────────────────────────────────────────┤
//! │ TIMESTAMP: u64 (Unix timestamp seconds)         │
//! ├─────────────────────────────────────────────────┤
//! │ ENTRY_COUNT: u64                                │
//! ├─────────────────────────────────────────────────┤
//! │ DATA_SIZE: u64 (uncompressed size)              │
//! ├─────────────────────────────────────────────────┤
//! │ RESERVED: [u8; 16]                              │
//! ├─────────────────────────────────────────────────┤
//! │                   DATA BLOCK                     │
//! │ (possibly LZ4 compressed)                       │
//! │ ┌─────────────────────────────────────────────┐ │
//! │ │ Entry 1:                                    │ │
//! │ │  - Key Length: u32                          │ │
//! │ │  - Key: [u8]                                │ │
//! │ │  - Value Length: u32                        │ │
//! │ │  - Value: [u8]                              │ │
//! │ │  - Has Expiration: u8 (0=No, 1=Yes)        │ │
//! │ │  - Expires At: u64 (if has_expiration=1)   │ │
//! │ ├─────────────────────────────────────────────┤ │
//! │ │ Entry 2: ...                                │ │
//! │ └─────────────────────────────────────────────┘ │
//! ├─────────────────────────────────────────────────┤
//! │ CRC32: u32                                      │
//! └─────────────────────────────────────────────────┘
//!
//! Total header size: 64 bytes
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Magic number for checkpoint files: "MCRS" (Moka Cache RS)
pub const MAGIC: [u8; 4] = [b'M', b'C', b'R', b'S'];

/// Current format version
pub const VERSION: u32 = 1;

/// Header size in bytes
pub const HEADER_SIZE: usize = 64;

/// Flag: data is LZ4 compressed
pub const FLAG_COMPRESSED: u32 = 1 << 0;

/// Checkpoint file header
#[derive(Debug, Clone)]
pub struct SnapshotHeader {
    /// Format version
    pub version: u32,

    /// Flags (compression, etc.)
    pub flags: u32,

    /// Raft log index at snapshot time
    pub raft_index: u64,

    /// Raft term at snapshot time
    pub raft_term: u64,

    /// Unix timestamp when snapshot was created
    pub timestamp: u64,

    /// Number of entries in the snapshot
    pub entry_count: u64,

    /// Uncompressed data size
    pub data_size: u64,
}

impl SnapshotHeader {
    /// Create a new header with the given Raft state
    pub fn new(raft_index: u64, raft_term: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            version: VERSION,
            flags: 0,
            raft_index,
            raft_term,
            timestamp,
            entry_count: 0,
            data_size: 0,
        }
    }

    /// Check if data is compressed
    pub fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }

    /// Set compression flag
    pub fn set_compressed(&mut self, compressed: bool) {
        if compressed {
            self.flags |= FLAG_COMPRESSED;
        } else {
            self.flags &= !FLAG_COMPRESSED;
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];

        // Magic (0-3)
        buf[0..4].copy_from_slice(&MAGIC);

        // Version (4-7)
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());

        // Flags (8-11)
        buf[8..12].copy_from_slice(&self.flags.to_le_bytes());

        // Raft index (12-19)
        buf[12..20].copy_from_slice(&self.raft_index.to_le_bytes());

        // Raft term (20-27)
        buf[20..28].copy_from_slice(&self.raft_term.to_le_bytes());

        // Timestamp (28-35)
        buf[28..36].copy_from_slice(&self.timestamp.to_le_bytes());

        // Entry count (36-43)
        buf[36..44].copy_from_slice(&self.entry_count.to_le_bytes());

        // Data size (44-51)
        buf[44..52].copy_from_slice(&self.data_size.to_le_bytes());

        // Reserved (52-63) - already zeros

        buf
    }

    /// Parse header from bytes
    pub fn from_bytes(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() < HEADER_SIZE {
            return Err(FormatError::InvalidHeader("header too short".into()));
        }

        // Check magic
        if buf[0..4] != MAGIC {
            return Err(FormatError::InvalidMagic);
        }

        // Parse fields
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version > VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }

        let flags = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let raft_index = u64::from_le_bytes(buf[12..20].try_into().unwrap());
        let raft_term = u64::from_le_bytes(buf[20..28].try_into().unwrap());
        let timestamp = u64::from_le_bytes(buf[28..36].try_into().unwrap());
        let entry_count = u64::from_le_bytes(buf[36..44].try_into().unwrap());
        let data_size = u64::from_le_bytes(buf[44..52].try_into().unwrap());

        Ok(Self {
            version,
            flags,
            raft_index,
            raft_term,
            timestamp,
            entry_count,
            data_size,
        })
    }
}

/// A single entry in the snapshot
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    /// Entry key
    pub key: Vec<u8>,

    /// Entry value
    pub value: Vec<u8>,

    /// Absolute expiration time (Unix timestamp in nanoseconds)
    pub expires_at: Option<u64>,
}

impl SnapshotEntry {
    /// Create a new entry without expiration
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            expires_at: None,
        }
    }

    /// Create a new entry with expiration
    pub fn with_expiration(key: Vec<u8>, value: Vec<u8>, expires_at: SystemTime) -> Self {
        let expires_nanos = expires_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            key,
            value,
            expires_at: Some(expires_nanos),
        }
    }

    /// Calculate the remaining TTL from now
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at.and_then(|expires_nanos| {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            if expires_nanos > now_nanos {
                Some(Duration::from_nanos(expires_nanos - now_nanos))
            } else {
                None // Already expired
            }
        })
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |expires_nanos| {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            expires_nanos <= now_nanos
        })
    }

    /// Serialized size of this entry
    pub fn serialized_size(&self) -> usize {
        4 + self.key.len() + // key length + key
        4 + self.value.len() + // value length + value
        1 + // has_expiration flag
        if self.expires_at.is_some() { 8 } else { 0 } // expires_at
    }

    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.serialized_size());

        // Key length + key
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key);

        // Value length + value
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.value);

        // Expiration
        if let Some(expires) = self.expires_at {
            buf.push(1);
            buf.extend_from_slice(&expires.to_le_bytes());
        } else {
            buf.push(0);
        }

        buf
    }

    /// Parse entry from bytes, returns (entry, bytes_consumed)
    pub fn from_bytes(buf: &[u8]) -> Result<(Self, usize), FormatError> {
        let mut pos = 0;

        // Key length + key
        if buf.len() < pos + 4 {
            return Err(FormatError::UnexpectedEof);
        }
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if buf.len() < pos + key_len {
            return Err(FormatError::UnexpectedEof);
        }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;

        // Value length + value
        if buf.len() < pos + 4 {
            return Err(FormatError::UnexpectedEof);
        }
        let value_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if buf.len() < pos + value_len {
            return Err(FormatError::UnexpectedEof);
        }
        let value = buf[pos..pos + value_len].to_vec();
        pos += value_len;

        // Expiration flag
        if buf.len() < pos + 1 {
            return Err(FormatError::UnexpectedEof);
        }
        let has_expiration = buf[pos] != 0;
        pos += 1;

        let expires_at = if has_expiration {
            if buf.len() < pos + 8 {
                return Err(FormatError::UnexpectedEof);
            }
            let expires = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
            pos += 8;
            Some(expires)
        } else {
            None
        };

        Ok((
            Self {
                key,
                value,
                expires_at,
            },
            pos,
        ))
    }
}

/// Format-related errors
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("invalid magic number")]
    InvalidMagic,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("invalid header: {0}")]
    InvalidHeader(String),

    #[error("unexpected end of file")]
    UnexpectedEof,

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let mut header = SnapshotHeader::new(100, 5);
        header.set_compressed(true);
        header.entry_count = 1000;
        header.data_size = 50000;

        let bytes = header.to_bytes();
        let parsed = SnapshotHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.version, VERSION);
        assert!(parsed.is_compressed());
        assert_eq!(parsed.raft_index, 100);
        assert_eq!(parsed.raft_term, 5);
        assert_eq!(parsed.entry_count, 1000);
        assert_eq!(parsed.data_size, 50000);
    }

    #[test]
    fn test_entry_roundtrip() {
        let entry = SnapshotEntry::new(b"key".to_vec(), b"value".to_vec());

        let bytes = entry.to_bytes();
        let (parsed, consumed) = SnapshotEntry::from_bytes(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.key, b"key");
        assert_eq!(parsed.value, b"value");
        assert!(parsed.expires_at.is_none());
    }

    #[test]
    fn test_entry_with_expiration() {
        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        let entry = SnapshotEntry::with_expiration(b"key".to_vec(), b"value".to_vec(), expires_at);

        let bytes = entry.to_bytes();
        let (parsed, _) = SnapshotEntry::from_bytes(&bytes).unwrap();

        assert!(parsed.expires_at.is_some());
        assert!(!parsed.is_expired());
        assert!(parsed.remaining_ttl().is_some());
    }

    #[test]
    fn test_invalid_magic() {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(b"XXXX");

        let result = SnapshotHeader::from_bytes(&bytes);
        assert!(matches!(result, Err(FormatError::InvalidMagic)));
    }
}
