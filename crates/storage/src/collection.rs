//! Collection management for NebulaDB storage

use std::path::{Path, PathBuf};
use std::fs;

use nebuladb_core::{Result, Error};

use crate::StorageConfig;
use crate::manager::BlockManager;

/// A collection in NebulaDB storage
#[derive(Debug)]
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
    
    /// Retrieve a document from the collection
    pub fn get(&self, id: &[u8]) -> Result<Option<Vec<u8>>> {
        // Use the block manager to find the document
        self.block_manager.find_document(id)
    }
    
    /// Delete a document from the collection
    pub fn delete(&mut self, _id: &[u8]) -> Result<bool> {
        // This is just a stub - it will need to be implemented
        // The implementation would likely mark the document as deleted
        // in its block and update indexes accordingly
        Err(Error::Other("Not implemented".to_string()))
    }
    
    /// Close the collection, flushing any pending changes
    pub fn close(&mut self) -> Result<()> {
        self.block_manager.flush()
    }
}
