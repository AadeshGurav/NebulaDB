use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock, Mutex};
use nebuladb_core::{Result, Error};
use nebuladb_storage::{StorageConfig, collection::Collection};
use nebuladb_wal::{WalConfig, manager::SharedWalManager, manager::WalManager};

/// A database in NebulaDB
#[derive(Clone)]
pub struct Database {
    /// Name of the database
    name: String,
    /// Path to the database files
    path: PathBuf,
    /// Configuration for the database
    config: StorageConfig,
    /// Open collections (synchronized for thread safety)
    collections: Arc<RwLock<HashMap<String, Arc<Mutex<Collection>>>>>,
    /// Write-ahead log manager for durability
    wal_manager: Option<SharedWalManager>,
    /// Maximum number of open collections
    max_open_collections: usize,
    /// Whether to use transactions
    use_transactions: bool,
}

impl Database {
    /// Create a new database
    pub fn new(name: &str, base_path: &Path, config: &StorageConfig) -> Result<Self> {
        let path = base_path.join(name);
        
        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| Error::IoError(e))?;
        }
        
        // Create WAL configuration
        let wal_dir = path.join("wal");
        let wal_config = WalConfig {
            dir_path: wal_dir.to_string_lossy().to_string(),
            max_file_size: 64 * 1024 * 1024, // 64MB
            sync_on_write: true,
            checkpoint_interval: 60, // Checkpoint every minute
        };
        
        // Initialize WAL manager
        let wal_manager = WalManager::new(wal_config)?;
        let shared_wal_manager = Arc::new(RwLock::new(wal_manager));
        
        Ok(Self {
            name: name.to_string(),
            path,
            config: config.clone(),
            collections: Arc::new(RwLock::new(HashMap::new())),
            wal_manager: Some(shared_wal_manager),
            max_open_collections: 100, // Default value
            use_transactions: true,    // Default to using transactions
        })
    }
    
    /// Configure database settings
    pub fn configure(&mut self, max_collections: usize, use_transactions: bool) {
        self.max_open_collections = max_collections;
        self.use_transactions = use_transactions;
    }
    
    /// Open or create a collection
    pub fn open_collection(&mut self, name: &str) -> Result<()> {
        // Check if we've hit the maximum open collections limit
        let current_count = self.collections.read().map_err(|_| 
            Error::Other("Failed to read collections lock".into()))?.len();
            
        if current_count >= self.max_open_collections {
            return Err(Error::Other(format!("Maximum number of open collections ({}) reached", 
                self.max_open_collections)));
        }
        
        // Check if the collection is already open
        if self.collections.read().map_err(|_| 
            Error::Other("Failed to read collections lock".into()))?.contains_key(name) {
            // Collection is already open
            return Ok(());
        }
        
        // Recover from WAL if available
        if let Some(wal_manager) = &self.wal_manager {
            if let Ok(mut wal_guard) = wal_manager.write() {
                // This will attempt to recover entries for the collection from the WAL
                println!("DEBUG: Attempting to recover from WAL for collection: {}", name);
                if let Err(e) = wal_guard.recover() {
                    println!("WARNING: Failed to recover from WAL: {:?}", e);
                }
            }
        }
        
        // Collection is not open, so open or create it
        let collection = Collection::open(name, &self.path, &self.config)?;
        
        // Wrap in Arc<Mutex> for thread safety
        let collection_mutex = Arc::new(Mutex::new(collection));
        
        // Add to open collections
        self.collections.write().map_err(|_| 
            Error::Other("Failed to write collections lock".into()))?
            .insert(name.to_string(), collection_mutex);
            
        Ok(())
    }
    
    /// Check if a collection exists
    pub fn collection_exists(&self, name: &str) -> bool {
        // First check in memory
        if self.collections.read().map_err(|_| ()).ok()
            .map_or(false, |c| c.contains_key(name)) {
            return true;
        }
        
        // Then check on disk
        let collection_path = self.path.join(name);
        if collection_path.exists() {
            let blocks_file = collection_path.join("blocks.bin");
            return blocks_file.exists();
        }
        
        false
    }
    
    /// Close a collection
    pub fn close_collection(&mut self, name: &str) -> Result<()> {
        let mut collections = self.collections.write().map_err(|_| 
            Error::Other("Failed to write collections lock".into()))?;
            
        if let Some(collection_mutex) = collections.remove(name) {
            // Get exclusive access to the collection
            let mut collection = collection_mutex.lock().map_err(|_| 
                Error::Other("Failed to lock collection for closing".into()))?;
                
            // Close the collection (flush data, etc.)
            collection.close()?;
        }
        
        Ok(())
    }
    
    /// Get a reference to an open collection
    pub fn get_collection(&self, name: &str) -> Option<Arc<Mutex<Collection>>> {
        self.collections.read().ok()?.get(name).cloned()
    }
    
    /// Get a mutable reference to an open collection
    pub fn get_collection_mut(&mut self, _name: &str) -> Option<&mut Collection> {
        // This is a limitation of the current design
        // In a real production system, we'd return the Arc<Mutex<Collection>> directly
        // and the caller would be responsible for locking it
        None
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<u64> {
        if !self.use_transactions {
            return Err(Error::Other("Transactions are disabled for this database".into()));
        }
        
        if let Some(wal) = &self.wal_manager {
            let mut wal_guard = wal.write().map_err(|_| 
                Error::Other("Failed to lock WAL manager".into()))?;
                
            wal_guard.begin_transaction()
        } else {
            Err(Error::Other("WAL manager not initialized".into()))
        }
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&mut self, tx_id: u64) -> Result<()> {
        if !self.use_transactions {
            return Err(Error::Other("Transactions are disabled for this database".into()));
        }
        
        if let Some(wal) = &self.wal_manager {
            let mut wal_guard = wal.write().map_err(|_| 
                Error::Other("Failed to lock WAL manager".into()))?;
                
            wal_guard.commit_transaction(tx_id)
        } else {
            Err(Error::Other("WAL manager not initialized".into()))
        }
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&mut self, tx_id: u64) -> Result<()> {
        if !self.use_transactions {
            return Err(Error::Other("Transactions are disabled for this database".into()));
        }
        
        if let Some(wal) = &self.wal_manager {
            let mut wal_guard = wal.write().map_err(|_| 
                Error::Other("Failed to lock WAL manager".into()))?;
                
            wal_guard.abort_transaction(tx_id)
        } else {
            Err(Error::Other("WAL manager not initialized".into()))
        }
    }
    
    /// List all collections (both open and on disk)
    pub fn list_collections(&self) -> Vec<String> {
        let mut collections = Vec::new();
        
        // First add all open collections
        if let Ok(coll_map) = self.collections.read() {
            for name in coll_map.keys() {
                collections.push(name.clone());
            }
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
        if let Ok(coll_map) = self.collections.read() {
            coll_map.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }
    
    /// Close all collections
    pub fn close_all_collections(&mut self) -> Result<()> {
        let mut last_error = None;
        
        // Get all collection names first
        let collection_names = {
            let collections = self.collections.read().map_err(|_| 
                Error::Other("Failed to read collections lock".into()))?;
            collections.keys().cloned().collect::<Vec<_>>()
        };
        
        // Now close each collection
        for name in collection_names {
            if let Err(e) = self.close_collection(&name) {
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
    
    /// Create a collection without opening it
    pub fn create_collection(&self, name: &str) -> Result<()> {
        if self.collection_exists(name) {
            return Err(Error::Other(format!("Collection '{}' already exists", name)));
        }
        
        // Create the collection directory
        let collection_path = self.path.join(name);
        fs::create_dir_all(&collection_path).map_err(|e| Error::IoError(e))?;
        
        // Create an empty blocks file
        let blocks_file = collection_path.join("blocks.bin");
        fs::File::create(blocks_file).map_err(|e| Error::IoError(e))?;
        
        Ok(())
    }
    
    /// Get the name of the database
    pub fn get_name(&self) -> &str {
        &self.name
    }
} 