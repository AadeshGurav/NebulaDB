use std::path::{Path, PathBuf};
use nebuladb_core::{Result, Error};
use nebuladb_storage::StorageConfig;
use crate::storage::Storage;
use crate::database::Database;
use serde_json::Value as JsonValue;

/// The database manager that handles interfaces to the storage engine
pub struct DatabaseManager {
    /// Storage engine instance
    storage: Storage,
}

impl DatabaseManager {
    /// Create a new database manager
    pub fn new(data_dir: &Path, config: StorageConfig) -> Result<Self> {
        Ok(Self {
            storage: Storage::new(data_dir, config)?,
        })
    }
    
    // DATABASE OPERATIONS
    
    /// Create a new database
    pub fn create_database(&mut self, name: &str) -> Result<()> {
        // Implementation to be added
        self.storage.create_database(name)
    }
    
    /// List all databases
    pub fn list_databases(&self) -> Result<Vec<String>> {
        self.storage.list_databases()
    }
    
    /// Set the active database
    pub fn use_database(&mut self, name: &str) -> Result<()> {
        self.storage.set_active_database(name)
    }
    
    /// Get the currently active database name
    pub fn get_active_database(&self) -> Option<String> {
        self.storage.get_active_database()
    }
    
    // COLLECTION OPERATIONS
    
    /// Create a new collection in the active database
    pub fn create_collection(&mut self, name: &str) -> Result<()> {
        self.storage.create_collection(name)
    }
    
    /// List collections in the active database
    pub fn list_collections(&self) -> Result<Vec<String>> {
        self.storage.list_collections()
    }
    
    /// Open a collection in the active database
    pub fn open_collection(&mut self, name: &str) -> Result<()> {
        self.storage.open_collection(name)
    }
    
    /// Close a collection in the active database
    pub fn close_collection(&mut self, name: &str) -> Result<()> {
        self.storage.close_collection(name)
    }
    
    // DOCUMENT OPERATIONS
    
    /// Insert a document into a collection
    pub fn insert_document(&mut self, collection: &str, id: &[u8], data: &[u8]) -> Result<()> {
        self.storage.insert_document(collection, id, data)
    }
    
    /// Insert a JSON document into a collection
    pub fn insert_json_document(&mut self, collection: &str, id: &[u8], json: &str) -> Result<()> {
        self.storage.insert_document(collection, id, json.as_bytes())
    }
    
    /// Get a document from a collection
    pub fn get_document(&self, collection: &str, id: &[u8]) -> Result<Option<Vec<u8>>> {
        self.storage.get_document(collection, id)
    }
    
    /// Delete a document from a collection
    pub fn delete_document(&mut self, collection: &str, id: &[u8]) -> Result<bool> {
        self.storage.delete_document(collection, id)
    }
    
    /// Find documents in a collection matching a query
    pub fn find_documents(&self, collection: &str, query: &JsonValue) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.storage.find_documents(collection, query)
    }
    
    /// Scan all documents in a collection
    pub fn scan_documents(&self, collection: &str) -> Result<Vec<Vec<u8>>> {
        self.storage.scan_documents(collection)
    }
    
    // MAINTENANCE OPERATIONS
    
    /// Set the compression type
    pub fn set_compression(&mut self, compression_type: &str) -> Result<()> {
        self.storage.set_compression(compression_type)
    }
    
    /// Enable WAL
    pub fn enable_wal(&mut self, wal_dir: Option<&Path>) -> Result<()> {
        self.storage.enable_wal(wal_dir)
    }
    
    /// Perform a checkpoint
    pub fn checkpoint(&mut self) -> Result<()> {
        self.storage.checkpoint()
    }
    
    /// Close all databases and collections
    pub fn close_all(&mut self) -> Result<()> {
        self.storage.close_all()
    }
}
