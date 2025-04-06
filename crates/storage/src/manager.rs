//! Block manager for NebulaDB storage engine

use crate::{Block, BlockHeader, BlockFooter, CompressionType, StorageConfig, Result};
use nebuladb_core::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Manages storage blocks for a collection
pub struct BlockManager {
    /// Configuration for the storage engine
    config: StorageConfig,
    /// Path to the collection directory
    collection_path: PathBuf,
    /// Current active block for writing
    active_block: Option<Block>,
    /// Number of documents in the active block
    doc_count: u32,
    /// Total uncompressed size of documents in the active block
    uncompressed_size: u64,
}

impl BlockManager {
    /// Create a new block manager for the given collection
    pub fn new(config: StorageConfig, collection_name: &str) -> Result<Self> {
        let collection_path = Path::new(&config.base.data_dir).join(collection_name);
        
        // Create the collection directory if it doesn't exist
        std::fs::create_dir_all(&collection_path)
            .map_err(|e| Error::IoError(e))?;
        
        Ok(Self {
            config,
            collection_path,
            active_block: None,
            doc_count: 0,
            uncompressed_size: 0,
        })
    }
    
    /// Get the path to the data file for the given block index
    fn data_file_path(&self, block_index: u32) -> PathBuf {
        self.collection_path.join(format!("data_{:08}.nbl", block_index))
    }
    
    /// Initialize a new active block
    fn init_active_block(&mut self) -> Result<()> {
        let block = Block::new(self.config.compression);
        self.active_block = Some(block);
        self.doc_count = 0;
        self.uncompressed_size = 0;
        Ok(())
    }
    
    /// Add a document to the active block
    pub fn add_document(&mut self, doc_id: &[u8], doc_data: &[u8]) -> Result<()> {
        // Initialize active block if needed
        if self.active_block.is_none() {
            self.init_active_block()?;
        }
        
        let block = self.active_block.as_mut().unwrap();
        
        // Format: [doc_id_len(2)][doc_id][doc_data_len(4)][doc_data]
        let doc_id_len = doc_id.len() as u16;
        let doc_data_len = doc_data.len() as u32;
        
        // Calculate total size
        let total_size = 2 + doc_id.len() + 4 + doc_data.len();
        
        // Check if we need to flush the current block (either due to size or count)
        if self.uncompressed_size + total_size as u64 > self.config.block_size as u64 ||
           self.doc_count >= self.config.flush_threshold {
            self.flush()?;
            self.init_active_block()?;
        }
        
        // Append document to active block data (uncompressed for now)
        // In a real implementation, this would be compressed
        let block = self.active_block.as_mut().unwrap();
        
        // Write doc_id length (2 bytes)
        block.data.extend_from_slice(&doc_id_len.to_le_bytes());
        // Write doc_id
        block.data.extend_from_slice(doc_id);
        // Write doc_data length (4 bytes)
        block.data.extend_from_slice(&doc_data_len.to_le_bytes());
        // Write doc_data
        block.data.extend_from_slice(doc_data);
        
        // Update counts
        self.doc_count += 1;
        self.uncompressed_size += total_size as u64;
        
        Ok(())
    }
    
    /// Flush the active block to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(block) = self.active_block.as_mut() {
            // In a real implementation, we would compress the data here
            // and compute the checksum
            
            // Update header
            block.header.doc_count = self.doc_count;
            block.header.uncompressed_size = self.uncompressed_size;
            block.header.compressed_size = block.data.len() as u64;
            
            // Compute a simple checksum (in reality, use CRC32 or similar)
            let checksum = block.data.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
            block.footer.checksum = checksum;
            
            // Find the next available block index
            let mut block_index = 0;
            while self.data_file_path(block_index).exists() {
                block_index += 1;
            }
            
            // Write to file
            let path = self.data_file_path(block_index);
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| Error::IoError(e))?;
            
            // Write header
            file.write_all(&block.header.magic).map_err(|e| Error::IoError(e))?;
            file.write_all(&[block.header.version]).map_err(|e| Error::IoError(e))?;
            file.write_all(&[block.header.compression as u8]).map_err(|e| Error::IoError(e))?;
            file.write_all(&block.header.doc_count.to_le_bytes()).map_err(|e| Error::IoError(e))?;
            file.write_all(&block.header.uncompressed_size.to_le_bytes()).map_err(|e| Error::IoError(e))?;
            file.write_all(&block.header.compressed_size.to_le_bytes()).map_err(|e| Error::IoError(e))?;
            file.write_all(&block.header.created_at.to_le_bytes()).map_err(|e| Error::IoError(e))?;
            
            // Write data
            file.write_all(&block.data).map_err(|e| Error::IoError(e))?;
            
            // Write footer
            file.write_all(&block.footer.checksum.to_le_bytes()).map_err(|e| Error::IoError(e))?;
            file.write_all(&block.footer.magic).map_err(|e| Error::IoError(e))?;
            
            // Reset active block
            self.active_block = None;
            self.doc_count = 0;
            self.uncompressed_size = 0;
        }
        
        Ok(())
    }
    
    /// Read a document from a block
    pub fn read_document(&self, block_index: u32, offset: usize) -> Result<Vec<u8>> {
        let path = self.data_file_path(block_index);
        
        // Open the file
        let mut file = File::open(&path).map_err(|e| Error::IoError(e))?;
        
        // Read the header to get necessary info
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(|e| Error::IoError(e))?;
        
        if magic != BlockHeader::MAGIC {
            return Err(Error::Other("Invalid block file format".to_string()));
        }
        
        // Skip the rest of the header
        file.seek(SeekFrom::Current((BlockHeader::SIZE - 4) as i64))
            .map_err(|e| Error::IoError(e))?;
        
        // Seek to the document offset
        file.seek(SeekFrom::Current(offset as i64))
            .map_err(|e| Error::IoError(e))?;
        
        // Read document ID length
        let mut id_len_bytes = [0u8; 2];
        file.read_exact(&mut id_len_bytes).map_err(|e| Error::IoError(e))?;
        let id_len = u16::from_le_bytes(id_len_bytes) as usize;
        
        // Skip document ID
        file.seek(SeekFrom::Current(id_len as i64))
            .map_err(|e| Error::IoError(e))?;
        
        // Read document data length
        let mut data_len_bytes = [0u8; 4];
        file.read_exact(&mut data_len_bytes).map_err(|e| Error::IoError(e))?;
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;
        
        // Read document data
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data).map_err(|e| Error::IoError(e))?;
        
        Ok(data)
    }
}

impl Drop for BlockManager {
    fn drop(&mut self) {
        // Ensure any active block is flushed on drop
        let _ = self.flush();
    }
}
