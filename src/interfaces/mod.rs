pub mod cli;
pub mod http;
pub mod grpc;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use nebuladb_core::{Result, Error};
use nebuladb_storage::StorageConfig;
use crate::database::Database;

#[derive(Clone)]
/// Interface manager that handles different ways to interact with the database
pub struct InterfaceManager {
    /// Base path for all databases
    base_path: Box<Path>,
    /// Storage configuration
    config: StorageConfig,
    /// Collection of databases (name -> database instance)
    databases: HashMap<String, Arc<Mutex<Database>>>,
    /// Currently active database name
    active_database: Option<String>,
    /// CLI interface (if enabled)
    cli: Option<Arc<Mutex<cli::CliInterface>>>,
    /// HTTP interface (if enabled)
    http: Option<http::HttpInterface>,
    /// gRPC interface (if enabled)
    grpc: Option<grpc::GrpcInterface>,
}

// Helper type to avoid recursive type issues
pub type InterfaceManagerRef = Arc<Mutex<InterfaceManager>>;

impl InterfaceManager {
    /// Create a new interface manager
    pub fn new(base_path: &Path, config: StorageConfig) -> Result<Self> {
        let mut manager = Self {
            base_path: base_path.into(),
            config,
            databases: HashMap::new(),
            active_database: None,
            cli: None,
            http: None,
            grpc: None,
        };
        
        // Look for existing databases
        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            // Skip hidden directories
                            if !name.starts_with('.') {
                                if let Ok(db) = Database::new(name, base_path, &manager.config) {
                                    manager.databases.insert(name.to_string(), Arc::new(Mutex::new(db)));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // If no databases found, create a default one
        if manager.databases.is_empty() {
            manager.create_database("default")?;
        }
        
        // Set the default database as active if none is active
        if manager.active_database.is_none() {
            if manager.databases.contains_key("default") {
                manager.active_database = Some("default".to_string());
            } else {
                // Otherwise, pick the first database
                if let Some(name) = manager.databases.keys().next() {
                    manager.active_database = Some(name.clone());
                }
            }
        }
        
        Ok(manager)
    }
    
    /// Get a reference to the active database
    pub fn get_active_database(&self) -> Result<Arc<Mutex<Database>>> {
        match &self.active_database {
            Some(name) => {
                match self.databases.get(name) {
                    Some(db) => Ok(Arc::clone(db)),
                    None => Err(Error::Other(format!("Active database '{}' not found", name))),
                }
            },
            None => Err(Error::Other("No active database".to_string())),
        }
    }
    
    /// Create a new database
    pub fn create_database(&mut self, name: &str) -> Result<()> {
        if self.databases.contains_key(name) {
            return Err(Error::Other(format!("Database '{}' already exists", name)));
        }
        
        let db = Database::new(name, &self.base_path, &self.config)?;
        self.databases.insert(name.to_string(), Arc::new(Mutex::new(db)));
        
        // If this is the first database, make it active
        if self.active_database.is_none() {
            self.active_database = Some(name.to_string());
        }
        
        Ok(())
    }
    
    /// List all available databases
    pub fn list_databases(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }
    
    /// Get the name of the active database
    pub fn get_active_database_name(&self) -> Option<String> {
        self.active_database.clone()
    }
    
    /// Set the active database
    pub fn set_active_database(&mut self, name: &str) -> Result<()> {
        if !self.databases.contains_key(name) {
            return Err(Error::Other(format!("Database '{}' does not exist", name)));
        }
        
        self.active_database = Some(name.to_string());
        Ok(())
    }
    
    /// Enable the CLI interface
    pub fn enable_cli(&mut self) -> Result<()> {
        // Create a shared reference to self
        let manager_ref = Arc::new(Mutex::new(self.clone()));
        
        // Create CLI interface with the shared reference
        let cli = cli::CliInterface::new(manager_ref)?;
        
        // Store CLI interface in a shared reference to break the recursive dependency
        self.cli = Some(Arc::new(Mutex::new(cli)));
        
        Ok(())
    }
    
    /// Enable the HTTP interface
    pub fn enable_http(&mut self, port: u16) -> Result<()> {
        self.http = Some(http::HttpInterface::new(self, port)?);
        Ok(())
    }
    
    /// Enable the gRPC interface
    pub fn enable_grpc(&mut self, port: u16) -> Result<()> {
        self.grpc = Some(grpc::GrpcInterface::new(self, port)?);
        Ok(())
    }
    
    /// Start all enabled interfaces
    pub fn start(&mut self) -> Result<()> {
        if let Some(cli_ref) = &self.cli {
            // Get mutable access to the CLI interface
            let mut cli = cli_ref.lock().map_err(|_| Error::Other("Failed to lock CLI interface".into()))?;
            cli.start()?;
        }
        
        if let Some(http) = &self.http {
            http.start()?;
        }
        
        if let Some(grpc) = &self.grpc {
            grpc.start()?;
        }
        
        Ok(())
    }
    
    /// Drop (delete) a database
    pub fn drop_database(&mut self, name: &str) -> Result<()> {
        // Check if the database exists
        if !self.databases.contains_key(name) {
            return Err(Error::Other(format!("Database '{}' does not exist", name)));
        }
        
        // Check if it's the active database
        if self.active_database.as_deref() == Some(name) {
            return Err(Error::Other(format!("Cannot delete active database '{}'", name)));
        }
        
        // Remove from memory
        if let Some(db_mutex) = self.databases.remove(name) {
            // Close all collections
            if let Ok(mut db) = db_mutex.lock() {
                db.close_all_collections()?;
            }
        }
        
        // Delete the directory
        let db_path = self.base_path.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path).map_err(|e| Error::IoError(e))?;
        }
        
        Ok(())
    }
}

impl Drop for InterfaceManager {
    fn drop(&mut self) {
        // Close all databases when the manager is dropped
        for (name, db_mutex) in &self.databases {
            if let Ok(mut db) = db_mutex.lock() {
                if let Err(e) = db.close_all_collections() {
                    eprintln!("Error closing collections in database '{}': {:?}", name, e);
                }
            }
        }
    }
} 