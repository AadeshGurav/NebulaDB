//! Block-level operations for NebulaDB storage

use crate::{Block, BlockHeader, BlockFooter, CompressionType, compression};
use nebuladb_core::{Error, Result};
use std::io::{Read, Write};

/// Document entry in a block
#[derive(Debug, Clone)]
pub struct DocumentEntry {
    /// Document ID
    pub id: Vec<u8>,
    /// Document data
    pub data: Vec<u8>,
    /// Offset of this document within the block data
    pub offset: usize,
}

impl DocumentEntry {
    /// Create a new document entry
    pub fn new(id: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            id,
            data,
            offset: 0,
        }
    }
    
    /// Size of this document entry in bytes
    pub fn size(&self) -> usize {
        // Format: [doc_id_len(2)][doc_id][doc_data_len(4)][doc_data]
        2 + self.id.len() + 4 + self.data.len()
    }
    
    /// Serialize this document entry to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.size());
        
        // Write doc_id length (2 bytes)
        bytes.extend_from_slice(&(self.id.len() as u16).to_le_bytes());
        // Write doc_id
        bytes.extend_from_slice(&self.id);
        // Write doc_data length (4 bytes)
        bytes.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        // Write doc_data
        bytes.extend_from_slice(&self.data);
        
        bytes
    }
    
    /// Deserialize a document entry from bytes
    pub fn from_bytes(bytes: &[u8], offset: usize) -> Result<Self> {
        if bytes.len() < 6 {
            return Err(Error::Other("Invalid document entry: too short".to_string()));
        }
        
        // Read doc_id length (2 bytes)
        let id_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        
        if bytes.len() < 2 + id_len + 4 {
            return Err(Error::Other("Invalid document entry: too short for ID".to_string()));
        }
        
        // Read doc_id
        let id = bytes[2..(2 + id_len)].to_vec();
        
        // Read doc_data length (4 bytes)
        let data_start = 2 + id_len;
        let data_len = u32::from_le_bytes([
            bytes[data_start],
            bytes[data_start + 1],
            bytes[data_start + 2],
            bytes[data_start + 3],
        ]) as usize;
        
        if bytes.len() < 2 + id_len + 4 + data_len {
            return Err(Error::Other("Invalid document entry: too short for data".to_string()));
        }
        
        // Read doc_data
        let data = bytes[(data_start + 4)..(data_start + 4 + data_len)].to_vec();
        
        Ok(Self {
            id,
            data,
            offset,
        })
    }
}

/// Operations for blocks
pub trait BlockOperations {
    /// Add a document to the block
    fn add_document(&mut self, doc: DocumentEntry) -> Result<()>;
    
    /// Get the number of documents in the block
    fn doc_count(&self) -> u32;
    
    /// Get the total size of the block in bytes
    fn size(&self) -> usize;
    
    /// Serialize the block to bytes
    fn to_bytes(&self) -> Result<Vec<u8>>;
    
    /// Deserialize a block from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self> where Self: Sized;
    
    /// Compute the checksum for the block
    fn compute_checksum(&self) -> u32;
}

impl BlockOperations for Block {
    fn add_document(&mut self, mut doc: DocumentEntry) -> Result<()> {
        // Set the offset for this document
        doc.offset = self.data.len();
        
        // Add the document to the block
        let doc_bytes = doc.to_bytes();
        self.data.extend_from_slice(&doc_bytes);
        
        // Update the header
        self.header.doc_count += 1;
        self.header.uncompressed_size += doc_bytes.len() as u64;
        
        // Update the checksum
        self.footer.checksum = self.compute_checksum();
        
        Ok(())
    }
    
    fn doc_count(&self) -> u32 {
        self.header.doc_count
    }
    
    fn size(&self) -> usize {
        BlockHeader::SIZE + self.data.len() + BlockFooter::SIZE
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(self.size());
        
        // Write header
        bytes.extend_from_slice(&self.header.magic);
        bytes.push(self.header.version);
        bytes.push(self.header.compression as u8);
        bytes.extend_from_slice(&self.header.doc_count.to_le_bytes());
        bytes.extend_from_slice(&self.header.uncompressed_size.to_le_bytes());
        bytes.extend_from_slice(&self.header.compressed_size.to_le_bytes());
        bytes.extend_from_slice(&self.header.created_at.to_le_bytes());
        
        // Write data
        bytes.extend_from_slice(&self.data);
        
        // Write footer
        bytes.extend_from_slice(&self.footer.checksum.to_le_bytes());
        bytes.extend_from_slice(&self.footer.magic);
        
        Ok(bytes)
    }
    
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < BlockHeader::SIZE + BlockFooter::SIZE {
            return Err(Error::Other("Invalid block: too short".to_string()));
        }
        
        // Read header
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        
        if magic != BlockHeader::MAGIC {
            return Err(Error::Other("Invalid block: wrong magic number".to_string()));
        }
        
        let version = bytes[4];
        let compression = match bytes[5] {
            0 => CompressionType::None,
            1 => CompressionType::Snappy,
            2 => CompressionType::Zstd,
            3 => CompressionType::Lz4,
            _ => return Err(Error::Other("Invalid compression type".to_string())),
        };
        
        let doc_count = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
        
        let uncompressed_size = u64::from_le_bytes([
            bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15], bytes[16], bytes[17],
        ]);
        
        let compressed_size = u64::from_le_bytes([
            bytes[18], bytes[19], bytes[20], bytes[21],
            bytes[22], bytes[23], bytes[24], bytes[25],
        ]);
        
        let created_at = u64::from_le_bytes([
            bytes[26], bytes[27], bytes[28], bytes[29],
            bytes[30], bytes[31], bytes[32], bytes[33],
        ]);
        
        // Read data
        let data_start = BlockHeader::SIZE;
        let data_end = bytes.len() - BlockFooter::SIZE;
        let data = bytes[data_start..data_end].to_vec();
        
        // Read footer
        let checksum = u32::from_le_bytes([
            bytes[data_end], bytes[data_end + 1], bytes[data_end + 2], bytes[data_end + 3],
        ]);
        
        let mut footer_magic = [0u8; 4];
        footer_magic.copy_from_slice(&bytes[data_end + 4..data_end + 8]);
        
        if footer_magic != BlockHeader::MAGIC {
            return Err(Error::Other("Invalid block: wrong footer magic number".to_string()));
        }
        
        let header = BlockHeader {
            magic,
            version,
            compression,
            doc_count,
            uncompressed_size,
            compressed_size,
            created_at,
        };
        
        let footer = BlockFooter {
            checksum,
            magic: footer_magic,
        };
        
        Ok(Self {
            header,
            data,
            footer,
        })
    }
    
    fn compute_checksum(&self) -> u32 {
        // In a real implementation, we would use CRC32 or a better algorithm
        // For simplicity, we'll just sum the bytes
        let mut sum = 0u32;
        
        // Include header in checksum
        sum = sum.wrapping_add(self.header.magic.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32)));
        sum = sum.wrapping_add(self.header.version as u32);
        sum = sum.wrapping_add(self.header.compression as u32);
        sum = sum.wrapping_add(self.header.doc_count);
        sum = sum.wrapping_add((self.header.uncompressed_size & 0xFFFFFFFF) as u32);
        sum = sum.wrapping_add((self.header.uncompressed_size >> 32) as u32);
        sum = sum.wrapping_add((self.header.compressed_size & 0xFFFFFFFF) as u32);
        sum = sum.wrapping_add((self.header.compressed_size >> 32) as u32);
        sum = sum.wrapping_add((self.header.created_at & 0xFFFFFFFF) as u32);
        sum = sum.wrapping_add((self.header.created_at >> 32) as u32);
        
        // Include data in checksum
        sum = sum.wrapping_add(self.data.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32)));
        
        sum
    }
}
