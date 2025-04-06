//! Block manager for NebulaDB storage

use std::fs::{File, OpenOptions};
use crate::{Block, StorageConfig, Result};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::PathBuf;
use crate::block::{BlockOperations, DocumentEntry};
use nebuladb_core::Error;

/// Maximum size of blocks in MB
pub const MAX_BLOCK_SIZE: usize = 4;

/// Block manager for a collection
pub struct BlockManager {
    /// Name of the collection
    name: String,
    /// Path to the collection files
    path: PathBuf,
    /// Configuration
    config: StorageConfig,
    /// Current active block
    active_block: Option<Block>,
    /// Current block index
    current_block_idx: u32,
    /// Base file path (collection/blocks.bin)
    base_file_path: PathBuf,
}

impl BlockManager {
    /// Create a new block manager
    pub fn new(name: &str, path: PathBuf, config: StorageConfig) -> Self {
        let base_file_path = path.join("blocks.bin");
        
        Self {
            name: name.to_string(),
            path,
            config,
            active_block: None,
            current_block_idx: 0,
            base_file_path,
        }
    }
    
    /// Ensure the active block is initialized
    fn ensure_active_block(&mut self) -> Result<()> {
        if self.active_block.is_none() {
            // Check if we have an existing block file
            if self.base_file_path.exists() {
                // If so, find the next block index
                self.current_block_idx = self.find_next_block_idx()?;
            }
            
            // Create a new block
            let block = Block::new(self.config.compression);
            self.active_block = Some(block);
        }
        
        Ok(())
    }
    
    /// Flush the current block to disk if it's past the threshold
    fn flush_if_needed(&mut self) -> Result<()> {
        if let Some(_) = self.active_block.as_ref() {
            // Check if we're past the threshold
            let block_size = self.active_block.as_ref().unwrap().size();
            if block_size >= self.config.flush_threshold as usize {
                self.flush()?;
            }
        }
        
        Ok(())
    }
    
    /// Flush the current block to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(block) = self.active_block.as_ref() {
            // Create a copy of the block before we use it
            let block_copy = block.clone();
            
            // Create or open the file
            let file_exists = self.base_file_path.exists();
            let mut file = if file_exists {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.base_file_path)
                    .map_err(|e| Error::Other(format!("Failed to open file: {}", e)))?
            } else {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&self.base_file_path)
                    .map_err(|e| Error::Other(format!("Failed to create file: {}", e)))?
            };
            
            // Calculate position in the file
            let block_size = block_copy.size() as u64;
            let position = self.current_block_idx as u64 * block_size;
            
            // Seek to the position
            file.seek(SeekFrom::Start(position))
                .map_err(|e| Error::Other(format!("Failed to seek in file: {}", e)))?;
            
            // Write the block header
            file.write_all(&block_copy.header.magic)
                .map_err(|e| Error::Other(format!("Failed to write header magic: {}", e)))?;
            file.write_all(&[block_copy.header.version])
                .map_err(|e| Error::Other(format!("Failed to write header version: {}", e)))?;
            file.write_all(&[block_copy.header.compression as u8])
                .map_err(|e| Error::Other(format!("Failed to write header compression: {}", e)))?;
            file.write_all(&block_copy.header.doc_count.to_le_bytes())
                .map_err(|e| Error::Other(format!("Failed to write doc count: {}", e)))?;
            file.write_all(&block_copy.header.uncompressed_size.to_le_bytes())
                .map_err(|e| Error::Other(format!("Failed to write uncompressed size: {}", e)))?;
            file.write_all(&block_copy.header.compressed_size.to_le_bytes())
                .map_err(|e| Error::Other(format!("Failed to write compressed size: {}", e)))?;
            file.write_all(&block_copy.header.created_at.to_le_bytes())
                .map_err(|e| Error::Other(format!("Failed to write created at: {}", e)))?;
            
            // Write the block data
            file.write_all(&block_copy.data)
                .map_err(|e| Error::Other(format!("Failed to write block data: {}", e)))?;
            
            // Write the block footer
            file.write_all(&block_copy.footer.checksum.to_le_bytes())
                .map_err(|e| Error::Other(format!("Failed to write footer checksum: {}", e)))?;
            file.write_all(&block_copy.footer.magic)
                .map_err(|e| Error::Other(format!("Failed to write footer magic: {}", e)))?;
            
            // Increment the block index and create a new active block
            self.current_block_idx += 1;
            self.active_block = Some(Block::new(self.config.compression));
            
            // Sync the file to disk
            file.sync_all()
                .map_err(|e| Error::Other(format!("Failed to sync file: {}", e)))?;
        }
        
        Ok(())
    }
    
    /// Find the next available block index
    fn find_next_block_idx(&self) -> Result<u32> {
        if !self.base_file_path.exists() {
            return Ok(0);
        }
        
        let file = File::open(&self.base_file_path)
            .map_err(|e| Error::Other(format!("Failed to open file: {}", e)))?;
        let file_size = file.metadata()
            .map_err(|e| Error::Other(format!("Failed to get metadata: {}", e)))?.len();
        
        if file_size == 0 {
            return Ok(0);
        }
        
        // For simplicity, we assume all blocks are of the same size
        // In a real implementation, we would need to read the headers
        // to determine the actual block sizes
        let block = Block::new(self.config.compression);
        let block_size = block.size() as u64;
        
        let num_blocks = file_size / block_size;
        
        Ok(num_blocks as u32)
    }
    
    /// Insert a document into the block manager
    pub fn insert(&mut self, id: &[u8], data: &[u8]) -> Result<()> {
        // Ensure we have an active block
        self.ensure_active_block()?;
        
        // Create a document entry
        let doc = DocumentEntry::new(id.to_vec(), data.to_vec());
        
        // Add the document to the active block
        if let Some(block) = self.active_block.as_mut() {
            block.add_document(doc)?;
        }
        
        // Flush if needed
        self.flush_if_needed()?;
        
        Ok(())
    }
    
    /// Read a document from a block
    pub fn read_document(&self, block_index: u32, offset: usize) -> Result<Vec<u8>> {
        let path = &self.base_file_path;
        
        // Open the file
        let mut file = File::open(path)
            .map_err(|e| Error::Other(format!("Failed to open file: {}", e)))?;
        
        // Calculate position in the file
        let block = Block::new(self.config.compression);
        let block_size = block.size() as u64;
        let position = block_index as u64 * block_size;
        
        // Seek to the block
        file.seek(SeekFrom::Start(position))
            .map_err(|e| Error::Other(format!("Failed to seek in file: {}", e)))?;
        
        // Read the block header
        let mut header_bytes = vec![0u8; crate::BlockHeader::SIZE];
        file.read_exact(&mut header_bytes)
            .map_err(|e| Error::Other(format!("Failed to read header: {}", e)))?;
        
        // Seek to the document offset within the block
        file.seek(SeekFrom::Start(position + crate::BlockHeader::SIZE as u64 + offset as u64))
            .map_err(|e| Error::Other(format!("Failed to seek to document: {}", e)))?;
        
        // Read document ID length
        let mut id_len_bytes = [0u8; 2];
        file.read_exact(&mut id_len_bytes)
            .map_err(|e| Error::Other(format!("Failed to read ID length: {}", e)))?;
        let id_len = u16::from_le_bytes(id_len_bytes) as usize;
        
        // Skip document ID
        file.seek(SeekFrom::Current(id_len as i64))
            .map_err(|e| Error::Other(format!("Failed to seek past ID: {}", e)))?;
        
        // Read document data length
        let mut data_len_bytes = [0u8; 4];
        file.read_exact(&mut data_len_bytes)
            .map_err(|e| Error::Other(format!("Failed to read data length: {}", e)))?;
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;
        
        // Read document data
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data)
            .map_err(|e| Error::Other(format!("Failed to read data: {}", e)))?;
        
        Ok(data)
    }
}
