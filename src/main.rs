use std::path::Path;
use nebuladb_core::Result;
use nebuladb_storage::StorageConfig;
use crate::interfaces::InterfaceManager;

mod database;
mod interfaces;
mod util;

fn main() -> Result<()> {
    // Create a data directory
    let data_dir = Path::new("./data");
    
    // Create storage config
    let config = StorageConfig::default();
    
    // Create interface manager
    let mut manager = InterfaceManager::new(data_dir, config)?;
    
    // Enable CLI interface
    manager.enable_cli()?;
    
    // Start all enabled interfaces
    manager.start()?;
    
    Ok(())
}
