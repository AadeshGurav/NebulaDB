//! File management utilities for NebulaDB storage

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use nebuladb_core::{Error, Result};

/// Interface for file operations
pub struct FileManager {
    /// Base directory for all data files
    data_dir: PathBuf,
}

impl FileManager {
    /// Create a new file manager with the given data directory
    pub fn new(data_dir: &str) -> Result<Self> {
        let data_dir = PathBuf::from(data_dir);
        
        // Create the data directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| Error::IoError(e))?;
        
        Ok(Self { data_dir })
    }
    
    /// Get the full path for a collection
    pub fn collection_path(&self, collection_name: &str) -> PathBuf {
        self.data_dir.join(collection_name)
    }
    
    /// Create a new collection directory
    pub fn create_collection(&self, collection_name: &str) -> Result<()> {
        let path = self.collection_path(collection_name);
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::IoError(e))?;
        
        Ok(())
    }
    
    /// Check if a collection exists
    pub fn collection_exists(&self, collection_name: &str) -> bool {
        let path = self.collection_path(collection_name);
        path.exists() && path.is_dir()
    }
    
    /// List all collections
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let entries = std::fs::read_dir(&self.data_dir)
            .map_err(|e| Error::IoError(e))?;
        
        let mut collections = Vec::new();
        
        for entry in entries {
            let entry = entry.map_err(|e| Error::IoError(e))?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        collections.push(name_str.to_string());
                    }
                }
            }
        }
        
        Ok(collections)
    }
    
    /// Create a new file
    pub fn create_file(&self, collection_name: &str, file_name: &str) -> Result<File> {
        let path = self.collection_path(collection_name).join(file_name);
        
        // Ensure the parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::IoError(e))?;
        }
        
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::IoError(e))
    }
    
    /// Open an existing file
    pub fn open_file(&self, collection_name: &str, file_name: &str) -> Result<File> {
        let path = self.collection_path(collection_name).join(file_name);
        File::open(&path).map_err(|e| Error::IoError(e))
    }
    
    /// Delete a file
    pub fn delete_file(&self, collection_name: &str, file_name: &str) -> Result<()> {
        let path = self.collection_path(collection_name).join(file_name);
        std::fs::remove_file(&path).map_err(|e| Error::IoError(e))
    }
}
