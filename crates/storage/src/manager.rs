//! Block manager for NebulaDB storage

use std::fs::{File, OpenOptions};
use crate::{Block, BlockHeader, StorageConfig, Result};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::PathBuf;
use crate::block::{BlockOperations, DocumentEntry};
use nebuladb_core::Error;

/// Maximum size of blocks in MB
pub const MAX_BLOCK_SIZE: usize = 4;

/// Block manager for a collection
#[derive(Debug, Clone)]
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
    
    /// Find a document by ID
    pub fn find_document(&self, doc_id: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.base_file_path.exists() {
            return Ok(None);
        }
        
        // First, check active block if it exists
        if let Some(block) = &self.active_block {
            // Search the active block for the document
            let doc_data = self.search_block_for_document(block, doc_id)?;
            if doc_data.is_some() {
                return Ok(doc_data);
            }
        }
        
        // Open the file
        let mut file = File::open(&self.base_file_path)
            .map_err(|e| Error::Other(format!("Failed to open file: {}", e)))?;
        
        // Determine how many blocks are in the file
        let file_size = file.metadata()
            .map_err(|e| Error::Other(format!("Failed to get metadata: {}", e)))?.len();
        
        if file_size == 0 {
            return Ok(None);
        }
        
        // Read each block and search for the document
        // Start from the newest blocks (higher likelihood of finding the document)
        let mut block_idx = self.current_block_idx;
        while block_idx > 0 {
            block_idx -= 1;
            
            // Seek to the block
            let mock_block = Block::new(self.config.compression);
            let block_size = mock_block.size() as u64;
            let position = block_idx as u64 * block_size;
            
            if position >= file_size {
                continue;
            }
            
            file.seek(SeekFrom::Start(position))
                .map_err(|e| Error::Other(format!("Failed to seek in file: {}", e)))?;
            
            // Read the entire block
            let mut block_data = vec![0u8; block_size as usize];
            
            // Use read instead of read_exact to handle end of file gracefully
            let bytes_read = file.read(&mut block_data)
                .map_err(|e| Error::Other(format!("Failed to read block: {}", e)))?;
            
            if bytes_read < BlockHeader::SIZE {
                continue;
            }
            
            // Parse the block
            let block = match Block::from_bytes(&block_data[0..bytes_read]) {
                Ok(b) => b,
                Err(_) => continue, // Skip invalid blocks
            };
            
            // Search this block for the document
            let doc_data = self.search_block_for_document(&block, doc_id)?;
            if doc_data.is_some() {
                return Ok(doc_data);
            }
        }
        
        // Document not found
        Ok(None)
    }
    
    /// Search a block for a document with the given ID
    fn search_block_for_document(&self, block: &Block, doc_id: &[u8]) -> Result<Option<Vec<u8>>> {
        // If the block is empty, return None
        if block.data.is_empty() {
            return Ok(None);
        }
        
        let mut offset = 0;
        
        // Iterate through document entries in the block
        while offset < block.data.len() {
            // Check if we have enough data for an ID length
            if offset + 2 > block.data.len() {
                break;
            }
            
            // Read ID length
            let id_len = u16::from_le_bytes([
                block.data[offset],
                block.data[offset + 1],
            ]) as usize;
            
            // Check if we have enough data for the ID
            if offset + 2 + id_len > block.data.len() {
                break;
            }
            
            // Read ID
            let entry_id = &block.data[offset + 2..offset + 2 + id_len];
            
            // Check if this is the document we're looking for
            if entry_id == doc_id {
                // Found the document, read its data
                let data_len_offset = offset + 2 + id_len;
                
                // Check if we have enough data for a data length
                if data_len_offset + 4 > block.data.len() {
                    break;
                }
                
                // Read data length
                let data_len = u32::from_le_bytes([
                    block.data[data_len_offset],
                    block.data[data_len_offset + 1],
                    block.data[data_len_offset + 2],
                    block.data[data_len_offset + 3],
                ]) as usize;
                
                // Check if we have enough data for the document
                if data_len_offset + 4 + data_len > block.data.len() {
                    break;
                }
                
                // Read document data
                let data = block.data[data_len_offset + 4..data_len_offset + 4 + data_len].to_vec();
                
                return Ok(Some(data));
            }
            
            // Move to the next document entry
            if offset + 2 + id_len + 4 > block.data.len() {
                break;
            }
            
            let data_len = u32::from_le_bytes([
                block.data[offset + 2 + id_len],
                block.data[offset + 2 + id_len + 1],
                block.data[offset + 2 + id_len + 2],
                block.data[offset + 2 + id_len + 3],
            ]) as usize;
            
            offset += 2 + id_len + 4 + data_len;
        }
        
        // Document not found in this block
        Ok(None)
    }
    
    /// Scan all blocks for document IDs
    pub fn scan_document_ids(&self) -> Result<Vec<Vec<u8>>> {
        let mut document_ids = Vec::new();
        
        // First scan the active block if it exists
        if let Some(block) = &self.active_block {
            let ids = self.scan_block_for_document_ids(block)?;
            document_ids.extend(ids);
        }
        
        // If no block file exists, return the results from the active block
        if !self.base_file_path.exists() {
            return Ok(document_ids);
        }
        
        // Open the file
        let mut file = File::open(&self.base_file_path)
            .map_err(|e| Error::Other(format!("Failed to open file: {}", e)))?;
        
        // Determine how many blocks are in the file
        let file_size = file.metadata()
            .map_err(|e| Error::Other(format!("Failed to get metadata: {}", e)))?.len();
        
        if file_size == 0 {
            return Ok(document_ids);
        }
        
        // Read each block and scan for document IDs
        for block_idx in 0..self.current_block_idx {
            // Seek to the block
            let mock_block = Block::new(self.config.compression);
            let block_size = mock_block.size() as u64;
            let position = block_idx as u64 * block_size;
            
            if position >= file_size {
                continue;
            }
            
            file.seek(SeekFrom::Start(position))
                .map_err(|e| Error::Other(format!("Failed to seek in file: {}", e)))?;
            
            // Read the entire block
            let mut block_data = vec![0u8; block_size as usize];
            
            // Use read instead of read_exact to handle end of file gracefully
            let bytes_read = file.read(&mut block_data)
                .map_err(|e| Error::Other(format!("Failed to read block: {}", e)))?;
            
            if bytes_read < BlockHeader::SIZE {
                continue;
            }
            
            // Parse the block
            let block = match Block::from_bytes(&block_data[0..bytes_read]) {
                Ok(b) => b,
                Err(_) => continue, // Skip invalid blocks
            };
            
            // Scan this block for document IDs
            let ids = self.scan_block_for_document_ids(&block)?;
            document_ids.extend(ids);
        }
        
        Ok(document_ids)
    }

    /// Scan a block for all document IDs
    fn scan_block_for_document_ids(&self, block: &Block) -> Result<Vec<Vec<u8>>> {
        let mut document_ids = Vec::new();
        
        // If the block is empty, return empty list
        if block.data.is_empty() {
            return Ok(document_ids);
        }
        
        let mut offset = 0;
        
        // Iterate through document entries in the block
        while offset < block.data.len() {
            // Check if we have enough data for an ID length
            if offset + 2 > block.data.len() {
                break;
            }
            
            // Read ID length
            let id_len = u16::from_le_bytes([
                block.data[offset],
                block.data[offset + 1],
            ]) as usize;
            
            // Check if we have enough data for the ID
            if offset + 2 + id_len > block.data.len() {
                break;
            }
            
            // Read ID
            let entry_id = block.data[offset + 2..offset + 2 + id_len].to_vec();
            
            // Add to our list if it's not a tombstone ID (doesn't start and end with underscore)
            if !(entry_id.starts_with(b"_") && entry_id.ends_with(b"_")) {
                document_ids.push(entry_id);
            }
            
            // Move to the next document entry
            if offset + 2 + id_len + 4 > block.data.len() {
                break;
            }
            
            let data_len = u32::from_le_bytes([
                block.data[offset + 2 + id_len],
                block.data[offset + 2 + id_len + 1],
                block.data[offset + 2 + id_len + 2],
                block.data[offset + 2 + id_len + 3],
            ]) as usize;
            
            offset += 2 + id_len + 4 + data_len;
        }
        
        Ok(document_ids)
    }
}
