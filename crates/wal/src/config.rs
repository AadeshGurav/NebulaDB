use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Path to the WAL directory
    pub dir_path: String,
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
            dir_path: "wal".to_string(),
            max_file_size: 64 * 1024 * 1024, // 64MB
            sync_on_write: true,
            checkpoint_interval: 300, // 5 minutes
        }
    }
} 