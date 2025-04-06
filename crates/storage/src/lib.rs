//! Storage engine for NebulaDB
//! 
//! This module handles the low-level storage of documents,
//! including block management, compression, and file operations.

pub mod block;
pub mod manager;
pub mod compression;
pub mod file;
pub mod wal_integration;
pub mod collection;

use nebuladb_core::{Result, Config};

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base configuration
    pub base: Config,
    /// Block size in bytes
    pub block_size: usize,
    /// Compression algorithm to use
    pub compression: CompressionType,
    /// Auto-flush threshold (in number of documents)
    pub flush_threshold: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base: Config::default(),
            block_size: 4 * 1024 * 1024, // 4MB blocks
            compression: CompressionType::Zstd,
            flush_threshold: 1000, // Flush every 1000 documents
        }
    }
}

/// Supported compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
}

/// Header for a data block
#[derive(Debug, Clone)]
pub struct BlockHeader {
    /// Magic number to identify NebulaDB blocks
    pub magic: [u8; 4],
    /// Block format version
    pub version: u8,
    /// Compression algorithm used
    pub compression: CompressionType,
    /// Number of documents in the block
    pub doc_count: u32,
    /// Total uncompressed size of all documents
    pub uncompressed_size: u64,
    /// Total compressed size of all documents
    pub compressed_size: u64,
    /// Timestamp when the block was created (UNIX timestamp)
    pub created_at: u64,
}

impl BlockHeader {
    /// Size of the block header in bytes
    pub const SIZE: usize = 4 + 1 + 1 + 4 + 8 + 8 + 8;
    
    /// Magic number for NebulaDB blocks: "NBLD"
    pub const MAGIC: [u8; 4] = [0x4E, 0x42, 0x4C, 0x44];
    
    /// Current version of the block format
    pub const VERSION: u8 = 1;
    
    /// Create a new block header
    pub fn new(
        compression: CompressionType,
        doc_count: u32,
        uncompressed_size: u64,
        compressed_size: u64,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            compression,
            doc_count,
            uncompressed_size,
            compressed_size,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

/// Footer for a data block
#[derive(Debug, Clone)]
pub struct BlockFooter {
    /// CRC32 checksum of the block (header + compressed data)
    pub checksum: u32,
    /// Magic number to identify NebulaDB blocks (same as header)
    pub magic: [u8; 4],
}

impl BlockFooter {
    /// Size of the block footer in bytes
    pub const SIZE: usize = 4 + 4;
    
    /// Create a new block footer with the given checksum
    pub fn new(checksum: u32) -> Self {
        Self {
            checksum,
            magic: BlockHeader::MAGIC,
        }
    }
}

/// Represents a document storage block
#[derive(Debug, Clone)]
pub struct Block {
    /// Block header
    pub header: BlockHeader,
    /// Compressed document data
    pub data: Vec<u8>,
    /// Block footer
    pub footer: BlockFooter,
}

impl Block {
    /// Create a new empty block with the given compression type
    pub fn new(compression: CompressionType) -> Self {
        let header = BlockHeader::new(compression, 0, 0, 0);
        let footer = BlockFooter::new(0); // Temporary checksum, will be updated
        
        Self {
            header,
            data: Vec::new(),
            footer,
        }
    }
    
    /// Get the total size of the block in bytes
    pub fn size(&self) -> usize {
        BlockHeader::SIZE + self.data.len() + BlockFooter::SIZE
    }
}
