//! Collection management for NebulaDB storage

use std::path::{Path, PathBuf};
use std::fs;

use nebuladb_core::{Result, Error};

use crate::StorageConfig;
use crate::manager::BlockManager;

/// A collection in NebulaDB storage
#[derive(Debug, Clone)]
pub struct Collection {
    /// Name of the collection
    pub name: String,
    /// Path to the collection files
    pub path: PathBuf,
    /// Block manager for this collection
    pub block_manager: BlockManager,
}

impl Collection {
    /// Open or create a collection
    pub fn open(name: &str, base_path: &Path, config: &StorageConfig) -> Result<Self> {
        let path = base_path.join(name);
        
        // Create directory if it doesn't exist
        if !path.exists() {
            fs::create_dir_all(&path).map_err(|e| Error::IoError(e))?;
        }
        
        let block_manager = BlockManager::new(name, path.clone(), config.clone());
        
        Ok(Self {
            name: name.to_string(),
            path,
            block_manager,
        })
    }
    
    /// Insert a document into the collection
    pub fn insert(&mut self, id: &[u8], data: &[u8]) -> Result<()> {
        self.block_manager.insert(id, data)
    }
    
    /// Get a list of all document IDs in the collection
    pub fn scan(&self) -> Result<Vec<Vec<u8>>> {
        self.block_manager.scan_document_ids()
    }
    
    /// Retrieve a document from the collection
    pub fn get(&self, id: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check if the document exists
        match self.block_manager.find_document(id)? {
            Some(data) => {
                // Check if this document has been logically deleted
                // by looking for a tombstone with a special ID
                let mut tombstone_id = Vec::with_capacity(id.len() + 2);
                tombstone_id.push(b'_');  // Prefix with underscore
                tombstone_id.extend_from_slice(id);
                tombstone_id.push(b'_');  // Suffix with underscore
                
                // If a tombstone exists, consider the document deleted
                match self.block_manager.find_document(&tombstone_id)? {
                    Some(_) => Ok(None), // Document was deleted
                    None => Ok(Some(data)), // Document exists and is not deleted
                }
            },
            None => Ok(None), // Document not found
        }
    }
    
    /// Delete a document from the collection
    pub fn delete(&mut self, id: &[u8]) -> Result<bool> {
        // In our initial implementation, we'll simply create a special
        // document entry that marks the original document as deleted
        
        // First, check if the document exists
        let exists = match self.get(id)? {
            Some(_) => true,
            None => false,
        };
        
        if !exists {
            return Ok(false); // Document not found
        }
        
        // Create a tombstone document (a special marker indicating deletion)
        // In a real implementation, you'd store this in a structured way
        let tombstone_data = format!("{{\"_deleted\": true, \"_id\": \"{}\", \"_deleted_at\": {}}}",
            String::from_utf8_lossy(id),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ).into_bytes();
        
        // Insert the tombstone with a special ID format to mark deletion
        let mut tombstone_id = Vec::with_capacity(id.len() + 2);
        tombstone_id.push(b'_');  // Prefix with underscore
        tombstone_id.extend_from_slice(id);
        tombstone_id.push(b'_');  // Suffix with underscore
        
        // Insert the tombstone
        self.block_manager.insert(&tombstone_id, &tombstone_data)?;
        
        // Note: This approach doesn't actually remove the original document,
        // it just adds a tombstone. A background job or compaction process
        // would be responsible for actually cleaning up deleted documents.
        
        Ok(true)
    }
    
    /// Close the collection, flushing any pending changes
    pub fn close(&mut self) -> Result<()> {
        self.block_manager.flush()
    }
}
