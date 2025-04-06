//! Write-Ahead Log (WAL) for NebulaDB
//! 
//! This module handles the Write-Ahead Logging for durability and crash recovery.
//! Each operation is logged before it's applied to the main storage.

mod entry;
mod log;
mod manager;

pub use entry::{WalEntry, EntryType, EntryHeader};
pub use log::WalLog;
pub use manager::WalManager;

use nebuladb_core::Config;

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Base configuration
    pub base: Config,
    /// Maximum size of a WAL file before rotation (in bytes)
    pub max_file_size: usize,
    /// Sync WAL to disk after every write
    pub sync_on_write: bool,
    /// Time interval between auto-checkpoints (in seconds, 0 to disable)
    pub checkpoint_interval: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            base: Config::default(),
            max_file_size: 64 * 1024 * 1024, // 64MB
            sync_on_write: true,
            checkpoint_interval: 300, // 5 minutes
        }
    }
}
