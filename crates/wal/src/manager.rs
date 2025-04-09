//! WAL manager for NebulaDB
//!
//! This module provides high-level WAL operations for collections.

use crate::{
    WalConfig,
    entry::{WalEntry, EntryType},
    log::WalLog,
};
use nebuladb_core::{Error, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime};

/// Helper function to generate a collection ID from a collection name
fn collection_id_from_name(name: &str) -> u64 {
    // A simple hash function for now
    // In a real implementation, we would use a better hash function
    let mut hash = 0u64;
    for byte in name.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }
    hash
}

/// A collection's WAL state
struct CollectionWal {
    /// The WAL log for this collection
    log: WalLog,
    /// Path to the WAL file
    path: PathBuf,
    /// Last checkpoint timestamp
    last_checkpoint: SystemTime,
    /// Current transaction ID counter
    next_tx_id: u64,
}

/// Manages WAL operations for multiple collections
pub struct WalManager {
    /// WAL configuration
    config: WalConfig,
    /// Base directory for WAL files
    wal_dir: PathBuf,
    /// Open WAL files by collection name
    collection_wals: HashMap<String, CollectionWal>,
    /// Active transactions
    active_transactions: HashMap<u64, Vec<u64>>, // tx_id -> list of entry positions
    /// In-memory WAL cache for fast recovery
    entry_cache: HashMap<(String, Vec<u8>), u64>, // (collection, doc_id) -> position
    /// Last auto-checkpoint time
    last_auto_checkpoint: Instant,
}

impl WalManager {
    /// Create a new WAL manager
    pub fn new(config: WalConfig) -> Result<Self> {
        let wal_dir = Path::new(&config.dir_path).to_path_buf();
        
        // Create the WAL directory if it doesn't exist
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| Error::IoError(e))?;
        
        Ok(Self {
            config,
            wal_dir,
            collection_wals: HashMap::new(),
            active_transactions: HashMap::new(),
            entry_cache: HashMap::new(),
            last_auto_checkpoint: Instant::now(),
        })
    }
    
    /// Get the WAL path for a collection
    fn wal_path(&self, collection_name: &str) -> PathBuf {
        self.wal_dir.join(format!("{}.wal", collection_name))
    }
    
    /// Open or create a WAL for a collection
    fn get_or_create_wal(&mut self, collection_name: &str) -> Result<&mut CollectionWal> {
        if !self.collection_wals.contains_key(collection_name) {
            let path = self.wal_path(collection_name);
            
            // Try to open existing WAL, or create a new one
            let log = if path.exists() {
                WalLog::open(&path, self.config.sync_on_write)?
            } else {
                WalLog::create(&path, self.config.sync_on_write)?
            };
            
            self.collection_wals.insert(collection_name.to_string(), CollectionWal {
                log,
                path,
                last_checkpoint: SystemTime::now(),
                next_tx_id: 1,
            });
        }
        
        // Check if we need an auto-checkpoint
        self.check_auto_checkpoint()?;
        
        // Return a mutable reference to the collection WAL
        Ok(self.collection_wals.get_mut(collection_name).unwrap())
    }
    
    /// Check if we should perform an auto-checkpoint
    fn check_auto_checkpoint(&mut self) -> Result<()> {
        if self.config.checkpoint_interval == 0 {
            return Ok(());
        }
        
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_auto_checkpoint).as_secs();
        
        if elapsed >= self.config.checkpoint_interval {
            // Perform checkpoint on all collections
            for name in self.collection_wals.keys().cloned().collect::<Vec<_>>() {
                self.checkpoint(&name)?;
            }
            
            self.last_auto_checkpoint = now;
        }
        
        Ok(())
    }
    
    /// Insert a document into the WAL
    pub fn insert(
        &mut self,
        collection_name: &str,
        document_id: &[u8],
        document_data: &[u8],
    ) -> Result<()> {
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Insert,
            collection_id,
            0, // Not transactional
            document_id.to_vec(),
            document_data.to_vec(),
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Update cache
        self.entry_cache.insert(
            (collection_name.to_string(), document_id.to_vec()),
            position,
        );
        
        Ok(())
    }
    
    /// Update a document in the WAL
    pub fn update(
        &mut self,
        collection_name: &str,
        document_id: &[u8],
        document_data: &[u8],
    ) -> Result<()> {
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Update,
            collection_id,
            0, // Not transactional
            document_id.to_vec(),
            document_data.to_vec(),
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Update cache
        self.entry_cache.insert(
            (collection_name.to_string(), document_id.to_vec()),
            position,
        );
        
        Ok(())
    }
    
    /// Delete a document from the WAL
    pub fn delete(
        &mut self,
        collection_name: &str,
        document_id: &[u8],
    ) -> Result<()> {
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Delete,
            collection_id,
            0, // Not transactional
            document_id.to_vec(),
            Vec::new(), // No data needed for delete
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Update cache
        self.entry_cache.insert(
            (collection_name.to_string(), document_id.to_vec()),
            position,
        );
        
        Ok(())
    }
    
    /// Begin a transaction
    pub fn begin_transaction(&mut self) -> Result<u64> {
        // Get the first collection to generate a tx ID
        // In a real implementation, we would use a global tx ID generator
        let collection_name = match self.collection_wals.keys().next() {
            Some(name) => name.clone(),
            None => {
                // No collections yet, create a dummy one
                let dummy_name = "_tx_manager";
                self.get_or_create_wal(dummy_name)?;
                dummy_name.to_string()
            }
        };
        
        let collection_wal = self.get_or_create_wal(&collection_name)?;
        let tx_id = collection_wal.next_tx_id;
        collection_wal.next_tx_id += 1;
        
        // Record the transaction start
        let entry = WalEntry::begin_tx(tx_id);
        let position = collection_wal.log.append(&entry)?;
        
        // Initialize transaction tracking
        self.active_transactions.insert(tx_id, vec![position]);
        
        Ok(tx_id)
    }
    
    /// Insert a document in a transaction
    pub fn insert_in_transaction(
        &mut self,
        tx_id: u64,
        collection_name: &str,
        document_id: &[u8],
        document_data: &[u8],
    ) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(Error::Other(format!("Transaction {} not active", tx_id)));
        }
        
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Insert,
            collection_id,
            tx_id,
            document_id.to_vec(),
            document_data.to_vec(),
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Track this entry in the transaction
        if let Some(entries) = self.active_transactions.get_mut(&tx_id) {
            entries.push(position);
        }
        
        Ok(())
    }
    
    /// Update a document in a transaction
    pub fn update_in_transaction(
        &mut self,
        tx_id: u64,
        collection_name: &str,
        document_id: &[u8],
        document_data: &[u8],
    ) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(Error::Other(format!("Transaction {} not active", tx_id)));
        }
        
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Update,
            collection_id,
            tx_id,
            document_id.to_vec(),
            document_data.to_vec(),
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Track this entry in the transaction
        if let Some(entries) = self.active_transactions.get_mut(&tx_id) {
            entries.push(position);
        }
        
        Ok(())
    }
    
    /// Delete a document in a transaction
    pub fn delete_in_transaction(
        &mut self,
        tx_id: u64,
        collection_name: &str,
        document_id: &[u8],
    ) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(Error::Other(format!("Transaction {} not active", tx_id)));
        }
        
        let collection_id = collection_id_from_name(collection_name);
        let collection_wal = self.get_or_create_wal(collection_name)?;
        
        let entry = WalEntry::new(
            EntryType::Delete,
            collection_id,
            tx_id,
            document_id.to_vec(),
            Vec::new(), // No data needed for delete
        );
        
        let position = collection_wal.log.append(&entry)?;
        
        // Track this entry in the transaction
        if let Some(entries) = self.active_transactions.get_mut(&tx_id) {
            entries.push(position);
        }
        
        Ok(())
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&mut self, tx_id: u64) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(Error::Other(format!("Transaction {} not active", tx_id)));
        }
        
        // Get the first collection to log the commit
        // In a real implementation, we would use a separate transaction log
        let collection_name = match self.collection_wals.keys().next() {
            Some(name) => name.clone(),
            None => return Err(Error::Other("No collections available".to_string())),
        };
        
        let collection_wal = self.get_or_create_wal(&collection_name)?;
        
        // Record the transaction commit
        let entry = WalEntry::commit_tx(tx_id);
        collection_wal.log.append(&entry)?;
        
        // Remove from active transactions
        self.active_transactions.remove(&tx_id);
        
        Ok(())
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&mut self, tx_id: u64) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(Error::Other(format!("Transaction {} not active", tx_id)));
        }
        
        // Get the first collection to log the abort
        // In a real implementation, we would use a separate transaction log
        let collection_name = match self.collection_wals.keys().next() {
            Some(name) => name.clone(),
            None => return Err(Error::Other("No collections available".to_string())),
        };
        
        let collection_wal = self.get_or_create_wal(&collection_name)?;
        
        // Record the transaction abort
        let entry = WalEntry::abort_tx(tx_id);
        collection_wal.log.append(&entry)?;
        
        // Remove from active transactions
        self.active_transactions.remove(&tx_id);
        
        Ok(())
    }
    
    /// Perform a checkpoint for a collection
    pub fn checkpoint(&mut self, collection_name: &str) -> Result<()> {
        let collection_wal = self.get_or_create_wal(collection_name)?;
        let collection_id = collection_id_from_name(collection_name);
        
        // Record a checkpoint entry
        let entry = WalEntry::checkpoint(collection_id);
        collection_wal.log.append(&entry)?;
        
        // Update checkpoint time
        collection_wal.last_checkpoint = SystemTime::now();
        
        // In a real implementation, we would:
        // 1. Ensure all data prior to this checkpoint is persisted to storage
        // 2. Potentially create a new WAL file and archive the old one
        
        Ok(())
    }
    
    /// Close all WAL files
    pub fn close(mut self) -> Result<()> {
        for (_, wal) in self.collection_wals.drain() {
            wal.log.close()?;
        }
        Ok(())
    }
    
    /// Recover from WAL files
    pub fn recover(&mut self) -> Result<()> {
        // Read WAL directory
        let entries = std::fs::read_dir(&self.wal_dir)
            .map_err(|e| Error::IoError(e))?;
        
        for entry in entries {
            let entry = entry.map_err(|e| Error::IoError(e))?;
            let path = entry.path();
            
            if path.extension() == Some(std::ffi::OsStr::new("wal")) {
                // Extract collection name from filename
                if let Some(filename) = path.file_stem() {
                    if let Some(collection_name) = filename.to_str() {
                        self.recover_collection(collection_name, &path)?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Recover a specific collection from its WAL
    fn recover_collection(&mut self, collection_name: &str, wal_path: &Path) -> Result<()> {
        let mut log = WalLog::open(wal_path, self.config.sync_on_write)?;
        
        // Iterate through all entries
        let mut valid_transactions = HashMap::new();
        let mut completed_transactions = HashMap::new();
        
        for result in log.iterate()? {
            let (position, entry) = result?;
            
            match entry.header.entry_type {
                EntryType::BeginTx => {
                    let tx_id = entry.header.transaction_id;
                    valid_transactions.insert(tx_id, true);
                }
                EntryType::CommitTx => {
                    let tx_id = entry.header.transaction_id;
                    completed_transactions.insert(tx_id, true);
                }
                EntryType::AbortTx => {
                    let tx_id = entry.header.transaction_id;
                    completed_transactions.insert(tx_id, false);
                }
                EntryType::Insert | EntryType::Update | EntryType::Delete => {
                    let tx_id = entry.header.transaction_id;
                    
                    // Only process non-transactional entries or entries in committed transactions
                    if tx_id == 0 || (completed_transactions.get(&tx_id) == Some(&true)) {
                        // Cache this entry
                        self.entry_cache.insert(
                            (collection_name.to_string(), entry.header.document_id.clone()),
                            position,
                        );
                    }
                }
                _ => {} // Ignore other entry types
            }
        }
        
        // In a real implementation, we would:
        // 1. Apply valid entries to storage
        // 2. Clean up aborted transactions
        
        // Add this WAL to the collection_wals map
        self.collection_wals.insert(collection_name.to_string(), CollectionWal {
            log,
            path: wal_path.to_path_buf(),
            last_checkpoint: SystemTime::now(),
            next_tx_id: valid_transactions.keys().max().unwrap_or(&0) + 1,
        });
        
        Ok(())
    }
}

// For thread safety in a real application
pub type SharedWalManager = Arc<RwLock<WalManager>>;
