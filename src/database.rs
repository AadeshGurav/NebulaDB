use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs;
use nebuladb_core::{Result, Error};
use nebuladb_storage::{StorageConfig, collection::Collection};

/// A database in NebulaDB
#[derive(Clone)]
pub struct Database {
    /// Name of the database
    name: String,
    /// Path to the database files
    path: PathBuf,
    /// Configuration for the database
    config: StorageConfig,
    /// Open collections
    collections: HashMap<String, Collection>,
}

impl Database {
    /// Create a new database
    pub fn new(name: &str, base_path: &Path, config: &StorageConfig) -> Result<Self> {
        let path = base_path.join(name);
        
        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| Error::IoError(e))?;
        }
        
        Ok(Self {
            name: name.to_string(),
            path,
            config: config.clone(),
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
    
    /// Get a reference to an open collection
    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }
    
    /// Get a mutable reference to an open collection
    pub fn get_collection_mut(&mut self, name: &str) -> Option<&mut Collection> {
        self.collections.get_mut(name)
    }
    
    /// List all collections (both open and on disk)
    pub fn list_collections(&self) -> Vec<String> {
        let mut collections = Vec::new();
        
        // First add all open collections
        for name in self.collections.keys() {
            collections.push(name.clone());
        }
        
        // Then scan the database directory for collections not currently open
        if let Ok(entries) = fs::read_dir(&self.path) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            // Check if this is a valid collection (has a blocks.bin file)
                            let blocks_file = entry.path().join("blocks.bin");
                            if blocks_file.exists() && !collections.contains(&name.to_string()) {
                                collections.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
        
        collections
    }
    
    /// List only currently open collections
    pub fn list_open_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }
    
    /// Close all collections
    pub fn close_all_collections(&mut self) -> Result<()> {
        let mut last_error = None;
        
        for (name, mut collection) in self.collections.drain() {
            if let Err(e) = collection.close() {
                eprintln!("Error closing collection '{}': {:?}", name, e);
                last_error = Some(e);
            }
        }
        
        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(())
        }
    }
    
    /// Check if a collection exists on disk
    pub fn collection_exists(&self, name: &str) -> bool {
        let collection_path = self.path.join(name);
        let blocks_file = collection_path.join("blocks.bin");
        collection_path.exists() && collection_path.is_dir() && blocks_file.exists()
    }
    
    /// Create a collection without opening it
    pub fn create_collection(&self, name: &str) -> Result<()> {
        let collection_path = self.path.join(name);
        
        // Create directory if it doesn't exist
        if !collection_path.exists() {
            std::fs::create_dir_all(&collection_path).map_err(|e| Error::IoError(e))?;
        }
        
        // Create an empty blocks file to mark this as a valid collection
        let blocks_file = collection_path.join("blocks.bin");
        if !blocks_file.exists() {
            std::fs::File::create(&blocks_file).map_err(|e| Error::IoError(e))?;
        }
        
        Ok(())
    }
} 