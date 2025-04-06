//! Archive module for NebulaDB
//!
//! This module will handle archiving of old/cold data.
//! For now, it's just a placeholder.

use nebuladb_core::{Error, Result};

/// Archive configuration
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// Archive threshold in days
    pub archive_threshold_days: u64,
    /// Archive directory
    pub archive_dir: String,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            archive_threshold_days: 90,
            archive_dir: String::from("/tmp/nebuladb/archive"),
        }
    }
}

/// A placeholder for archive functionality
pub fn placeholder() -> &'static str {
    "Archive module is not yet implemented"
}
