//! Main storage engine for NebulaDB

use std::path::{Path, PathBuf};
use std::fs;
use std::collections::HashMap;

use nebuladb_core::{Result, Error};

use crate::{Collection, StorageConfig};

/// Storage engine for NebulaDB
#[derive(Debug)]
pub struct Storage {
    /// Base path for storage
    pub path: PathBuf,
    /// Configuration for the storage engine
    pub config: StorageConfig,
    /// Open collections
    collections: HashMap<String, Collection>,
}

impl Storage {
    /// Open or create a storage engine at the given path
    pub fn open(path: &Path, config: Option<StorageConfig>) -> Result<Self> {
        // Create directory if it doesn't exist
        if !path.exists() {
            fs::create_dir_all(path)?;
        }
        
        let config = config.unwrap_or_default();
        
        Ok(Self {
            path: path.to_owned(),
            config,
            collections: HashMap::new(),
        })
    }
    
    /// Open or create a collection
    pub fn open_collection(&mut self, name: &str) -> Result<&mut Collection> {
        if self.collections.contains_key(name) {
            return Ok(self.collections.get_mut(name).unwrap());
        }
        
        let collection = Collection::open(name, &self.path, &self.config)?;
        self.collections.insert(name.to_string(), collection);
        
        Ok(self.collections.get_mut(name).unwrap())
    }
    
    /// Close a collection
    pub fn close_collection(&mut self, name: &str) -> Result<()> {
        if let Some(mut collection) = self.collections.remove(name) {
            collection.close()?;
        }
        
        Ok(())
    }
    
    /// Drop a collection
    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        // Close it first if it's open
        self.close_collection(name)?;
        
        // Delete the collection directory
        let path = self.path.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        
        Ok(())
    }
    
    /// Close the storage engine
    pub fn close(&mut self) -> Result<()> {
        // Close all collections
        let collection_names: Vec<String> = self.collections.keys().cloned().collect();
        for name in collection_names {
            self.close_collection(&name)?;
        }
        
        Ok(())
    }
} 