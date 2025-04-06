//! WAL entry definitions
//!
//! This module defines the structure and types of WAL entries.

use nebuladb_core::{Error, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// Types of WAL entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    /// No operation (used for padding)
    Noop = 0,
    /// Insert a document
    Insert = 1,
    /// Update a document
    Update = 2,
    /// Delete a document
    Delete = 3,
    /// Begin transaction
    BeginTx = 4,
    /// Commit transaction
    CommitTx = 5,
    /// Abort transaction
    AbortTx = 6,
    /// Checkpoint marker
    Checkpoint = 7,
}

impl EntryType {
    /// Convert a byte to an EntryType
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(EntryType::Noop),
            1 => Ok(EntryType::Insert),
            2 => Ok(EntryType::Update),
            3 => Ok(EntryType::Delete),
            4 => Ok(EntryType::BeginTx),
            5 => Ok(EntryType::CommitTx),
            6 => Ok(EntryType::AbortTx),
            7 => Ok(EntryType::Checkpoint),
            _ => Err(Error::Other(format!("Invalid WAL entry type: {}", byte))),
        }
    }
}

/// Header for a WAL entry
#[derive(Debug, Clone)]
pub struct EntryHeader {
    /// Magic number to identify NebulaDB WAL entries: "NBWL"
    pub magic: [u8; 4],
    /// Entry type
    pub entry_type: EntryType,
    /// Collection ID (hash of collection name)
    pub collection_id: u64,
    /// Transaction ID (0 for non-transactional operations)
    pub transaction_id: u64,
    /// Document ID
    pub document_id: Vec<u8>,
    /// Total size of the entry data
    pub data_size: u32,
    /// CRC32 checksum of the entry data
    pub checksum: u32,
    /// Timestamp when the entry was created (UNIX timestamp)
    pub timestamp: u64,
}

impl EntryHeader {
    /// Size of the fixed part of the header in bytes (excluding variable-length document ID)
    pub const FIXED_SIZE: usize = 4 + 1 + 8 + 8 + 2 + 4 + 4 + 8;
    
    /// Magic number for NebulaDB WAL entries: "NBWL"
    pub const MAGIC: [u8; 4] = [0x4E, 0x42, 0x57, 0x4C];
    
    /// Create a new entry header
    pub fn new(
        entry_type: EntryType,
        collection_id: u64,
        transaction_id: u64,
        document_id: Vec<u8>,
        data_size: u32,
        checksum: u32,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            entry_type,
            collection_id,
            transaction_id,
            document_id,
            data_size,
            checksum,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    
    /// Total size of the header in bytes (including variable-length document ID)
    pub fn size(&self) -> usize {
        Self::FIXED_SIZE + self.document_id.len()
    }
    
    /// Serialize the header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.size());
        
        // Magic number
        bytes.extend_from_slice(&self.magic);
        
        // Entry type
        bytes.push(self.entry_type as u8);
        
        // Collection ID
        bytes.extend_from_slice(&self.collection_id.to_le_bytes());
        
        // Transaction ID
        bytes.extend_from_slice(&self.transaction_id.to_le_bytes());
        
        // Document ID length (2 bytes)
        bytes.extend_from_slice(&(self.document_id.len() as u16).to_le_bytes());
        
        // Document ID
        bytes.extend_from_slice(&self.document_id);
        
        // Data size
        bytes.extend_from_slice(&self.data_size.to_le_bytes());
        
        // Checksum
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        
        // Timestamp
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        
        bytes
    }
    
    /// Deserialize a header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < Self::FIXED_SIZE - 2 { // -2 because doc_id_len is part of FIXED_SIZE
            return Err(Error::Other("Invalid WAL entry header: too short".to_string()));
        }
        
        let mut offset = 0;
        
        // Read magic number
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[offset..offset+4]);
        offset += 4;
        
        if magic != Self::MAGIC {
            return Err(Error::Other("Invalid WAL entry header: wrong magic number".to_string()));
        }
        
        // Read entry type
        let entry_type = EntryType::from_byte(bytes[offset])?;
        offset += 1;
        
        // Read collection ID
        let collection_id = u64::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
            bytes[offset+4], bytes[offset+5], bytes[offset+6], bytes[offset+7],
        ]);
        offset += 8;
        
        // Read transaction ID
        let transaction_id = u64::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
            bytes[offset+4], bytes[offset+5], bytes[offset+6], bytes[offset+7],
        ]);
        offset += 8;
        
        // Read document ID length
        let doc_id_len = u16::from_le_bytes([bytes[offset], bytes[offset+1]]) as usize;
        offset += 2;
        
        if bytes.len() < offset + doc_id_len + 16 { // 16 = remaining fixed fields
            return Err(Error::Other("Invalid WAL entry header: too short for document ID".to_string()));
        }
        
        // Read document ID
        let document_id = bytes[offset..offset+doc_id_len].to_vec();
        offset += doc_id_len;
        
        // Read data size
        let data_size = u32::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
        ]);
        offset += 4;
        
        // Read checksum
        let checksum = u32::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
        ]);
        offset += 4;
        
        // Read timestamp
        let timestamp = u64::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
            bytes[offset+4], bytes[offset+5], bytes[offset+6], bytes[offset+7],
        ]);
        offset += 8;
        
        let header = EntryHeader {
            magic,
            entry_type,
            collection_id,
            transaction_id,
            document_id,
            data_size,
            checksum,
            timestamp,
        };
        
        Ok((header, offset))
    }
}

/// A complete WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Entry header
    pub header: EntryHeader,
    /// Entry data
    pub data: Vec<u8>,
}

impl WalEntry {
    /// Create a new WAL entry
    pub fn new(
        entry_type: EntryType,
        collection_id: u64,
        transaction_id: u64,
        document_id: Vec<u8>,
        data: Vec<u8>,
    ) -> Self {
        // Calculate checksum (simple implementation - use CRC32 in production)
        let checksum = data.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
        
        let header = EntryHeader::new(
            entry_type,
            collection_id,
            transaction_id,
            document_id,
            data.len() as u32,
            checksum,
        );
        
        Self {
            header,
            data,
        }
    }
    
    /// Create a checkpoint entry
    pub fn checkpoint(collection_id: u64) -> Self {
        Self::new(
            EntryType::Checkpoint,
            collection_id,
            0,
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a transaction begin entry
    pub fn begin_tx(transaction_id: u64) -> Self {
        Self::new(
            EntryType::BeginTx,
            0,
            transaction_id,
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a transaction commit entry
    pub fn commit_tx(transaction_id: u64) -> Self {
        Self::new(
            EntryType::CommitTx,
            0,
            transaction_id,
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a transaction abort entry
    pub fn abort_tx(transaction_id: u64) -> Self {
        Self::new(
            EntryType::AbortTx,
            0,
            transaction_id,
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Total size of the entry in bytes
    pub fn size(&self) -> usize {
        self.header.size() + self.data.len()
    }
    
    /// Serialize the entry to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.size());
        
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.data);
        
        bytes
    }
    
    /// Deserialize an entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        let (header, offset) = EntryHeader::from_bytes(bytes)?;
        
        if bytes.len() < offset + header.data_size as usize {
            return Err(Error::Other("Invalid WAL entry: data too short".to_string()));
        }
        
        let data = bytes[offset..offset + header.data_size as usize].to_vec();
        
        // Verify checksum
        let checksum = data.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
        if checksum != header.checksum {
            return Err(Error::Other("Invalid WAL entry: checksum mismatch".to_string()));
        }
        
        let data_size = header.data_size;
        Ok((Self { header, data }, offset + data_size as usize))
    }
}
