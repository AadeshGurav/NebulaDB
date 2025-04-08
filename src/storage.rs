use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs;
use nebuladb_core::{Result, Error};
use nebuladb_storage::{StorageConfig, collection::Collection, CompressionType};
use crate::database::Database;
use serde_json::Value as JsonValue;

/// Storage engine that manages all databases
pub struct Storage {
    /// Base path for all databases
    path: Box<Path>,
    /// Configuration for the storage engine
    config: StorageConfig,
    /// Open databases
    databases: HashMap<String, Database>,
    /// Currently active database
    active_database: Option<String>,
}

impl Storage {
    /// Create a new storage engine
    pub fn new(path: &Path, config: StorageConfig) -> Result<Self> {
        // Create directory if it doesn't exist
        if !path.exists() {
            fs::create_dir_all(path).map_err(|e| Error::IoError(e))?;
        }
        
        Ok(Self {
            path: path.into(),
            config,
            databases: HashMap::new(),
            active_database: None,
        })
    }
    
    /// Create a new database
    pub fn create_database(&mut self, name: &str) -> Result<()> {
        if self.databases.contains_key(name) {
            return Err(Error::Other(format!("Database '{}' already exists", name)));
        }
        
        let db = Database::new(name, &self.path, &self.config)?;
        self.databases.insert(name.to_string(), db);
        
        Ok(())
    }
    
    /// List all databases
    pub fn list_databases(&self) -> Result<Vec<String>> {
        // First, list all directories in the base path
        let entries = fs::read_dir(&*self.path)
            .map_err(|e| Error::IoError(e))?;
        
        let mut dbs = Vec::new();
        
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        dbs.push(name.to_string());
                    }
                }
            }
        }
        
        Ok(dbs)
    }
    
    /// Set the active database
    pub fn set_active_database(&mut self, name: &str) -> Result<()> {
        // If database isn't loaded yet, load it
        if !self.databases.contains_key(name) {
            let db = Database::new(name, &self.path, &self.config)?;
            self.databases.insert(name.to_string(), db);
        }
        
        self.active_database = Some(name.to_string());
        Ok(())
    }
    
    /// Get the active database name
    pub fn get_active_database(&self) -> Option<String> {
        self.active_database.clone()
    }
    
    /// Create a new collection in the active database
    pub fn create_collection(&mut self, name: &str) -> Result<()> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        // Just opening the collection will create it if it doesn't exist
        db.open_collection(name, &self.config)?;
        
        Ok(())
    }
    
    /// List collections in the active database
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        Ok(db.list_collections())
    }
    
    /// Open a collection in the active database
    pub fn open_collection(&mut self, name: &str) -> Result<()> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        db.open_collection(name, &self.config)?;
        
        Ok(())
    }
    
    /// Close a collection in the active database
    pub fn close_collection(&mut self, name: &str) -> Result<()> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        db.close_collection(name)
    }
    
    // Forward document operations to the active database
    
    /// Insert a document into a collection
    pub fn insert_document(&mut self, collection: &str, id: &[u8], data: &[u8]) -> Result<()> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        let coll = db.get_collection_mut(collection)
            .ok_or_else(|| Error::Other(format!("Collection '{}' not found", collection)))?;
        
        coll.insert(id, data)
    }
    
    /// Get a document from a collection
    pub fn get_document(&self, collection: &str, id: &[u8]) -> Result<Option<Vec<u8>>> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        let coll = db.get_collection(collection)
            .ok_or_else(|| Error::Other(format!("Collection '{}' not found", collection)))?;
        
        coll.get(id)
    }
    
    /// Delete a document from a collection
    pub fn delete_document(&mut self, collection: &str, id: &[u8]) -> Result<bool> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        let coll = db.get_collection_mut(collection)
            .ok_or_else(|| Error::Other(format!("Collection '{}' not found", collection)))?;
        
        coll.delete(id)
    }
    
    /// Scan all document IDs in a collection
    pub fn scan_documents(&self, collection: &str) -> Result<Vec<Vec<u8>>> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        let coll = db.get_collection(collection)
            .ok_or_else(|| Error::Other(format!("Collection '{}' not found", collection)))?;
        
        coll.scan()
    }
    
    /// Find documents in a collection matching a query
    pub fn find_documents(&self, collection: &str, query: &JsonValue) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db_name = self.active_database.as_ref()
            .ok_or_else(|| Error::Other("No active database selected".into()))?;
        
        let db = self.databases.get(db_name)
            .ok_or_else(|| Error::Other(format!("Database '{}' not found", db_name)))?;
        
        let coll = db.get_collection(collection)
            .ok_or_else(|| Error::Other(format!("Collection '{}' not found", collection)))?;
        
        // Get all document IDs
        let ids = coll.scan()?;
        let mut results = Vec::new();
        
        // Scan through all documents and check if they match the query
        for id in &ids {
            if let Ok(Some(data)) = coll.get(id) {
                let data_str = String::from_utf8_lossy(&data);
                
                if let Ok(json_data) = serde_json::from_str::<JsonValue>(&data_str) {
                    // Simple matching - check if all query key-value pairs exist in the document
                    if let Some(query_obj) = query.as_object() {
                        if let Some(doc_obj) = json_data.as_object() {
                            let mut matches = true;
                            
                            for (key, val) in query_obj {
                                match doc_obj.get(key) {
                                    Some(doc_val) if doc_val == val => continue,
                                    _ => {
                                        matches = false;
                                        break;
                                    }
                                }
                            }
                            
                            if matches {
                                results.push((id.clone(), data));
                            }
                        }
                    }
                }
            }
        }
        
        Ok(results)
    }
    
    /// Set compression type for the storage engine
    pub fn set_compression(&mut self, compression_type: &str) -> Result<()> {
        // Convert string to compression type
        let compression = match compression_type.to_lowercase().as_str() {
            "none" => CompressionType::None,
            "snappy" => CompressionType::Snappy,
            "zstd" => CompressionType::Zstd,
            "lz4" => CompressionType::Lz4,
            _ => return Err(Error::Other(format!("Unsupported compression type: {}", compression_type))),
        };
        
        // Update the config
        self.config.compression = compression;
        
        Ok(())
    }
    
    /// Enable WAL for all collections
    pub fn enable_wal(&mut self, wal_dir: Option<&Path>) -> Result<()> {
        // Set the WAL directory (default to a 'wal' subdirectory in the data path)
        let wal_path = match wal_dir {
            Some(path) => path.to_path_buf(),
            None => PathBuf::from(&*self.path).join("wal"),
        };
        
        // Create WAL directory if it doesn't exist
        if !wal_path.exists() {
            fs::create_dir_all(&wal_path).map_err(|e| Error::IoError(e))?;
        }
        
        // In a real implementation, this would set up WAL integration
        // with each database/collection
        
        Ok(())
    }
    
    /// Perform a checkpoint (sync WAL to storage)
    pub fn checkpoint(&mut self) -> Result<()> {
        // In a real implementation, this would flush the WAL
        // and perform a checkpoint operation
        
        Ok(())
    }
    
    /// Close all databases
    pub fn close_all(&mut self) -> Result<()> {
        let mut last_error = None;
        
        for (name, db) in self.databases.drain() {
            // In a real implementation, we'd close each database
            if let Err(e) = self.close_database(&name) {
                eprintln!("Error closing database '{}': {:?}", name, e);
                last_error = Some(e);
            }
        }
        
        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(())
        }
    }
    
    /// Close a specific database
    fn close_database(&mut self, name: &str) -> Result<()> {
        if let Some(mut db) = self.databases.remove(name) {
            db.close_all_collections()?;
        }
        
        Ok(())
    }
}
