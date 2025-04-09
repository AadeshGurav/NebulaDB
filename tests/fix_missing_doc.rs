//! Test program to diagnose document retrieval issues

use std::path::Path;
use nebuladb_storage::collection::Collection;
use nebuladb_storage::{StorageConfig, CompressionType};
use nebuladb_core::{Config, Error as CoreError};

// Create a wrapper error type that implements std::error::Error
#[derive(Debug)]
struct TestError(CoreError);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::error::Error for TestError {}

impl From<CoreError> for TestError {
    fn from(err: CoreError) -> Self {
        TestError(err)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("data/abbc");
    let collection_name = "ccc";
    
    println!("Opening collection {} in {}", collection_name, db_path.display());
    
    // Create storage config with proper base Config
    let config = StorageConfig {
        base: Config {
            data_dir: db_path.to_string_lossy().to_string(),
            max_size: 16384,
        },
        compression: CompressionType::None,
        flush_threshold: 4096,
        block_size: 4096,
    };
    
    // Open the collection
    let mut collection = Collection::open(collection_name, db_path, &config)
        .map_err(TestError::from)?;
    
    // First, let's try to read the document ID "1"
    let doc_id = b"1";  // Use byte slice instead of [u8]
    
    match collection.get(doc_id) {
        Ok(Some(data)) => {
            println!("Successfully found document: {}", String::from_utf8_lossy(&data));
        },
        Ok(None) => {
            println!("Document not found, attempting to repair...");
            
            // Try to insert the document again
            let doc_content = br#"{"A": 1}"#;  // Use byte slice instead of [u8]
            println!("Reinserting document with ID '1'");
            collection.insert(doc_id, doc_content)
                .map_err(TestError::from)?;
            
            // Force flush the active block
            collection.block_manager.flush()
                .map_err(TestError::from)?;
            
            // Try to read it again
            match collection.get(doc_id) {
                Ok(Some(data)) => {
                    println!("Document repair successful: {}", String::from_utf8_lossy(&data));
                },
                Ok(None) => {
                    println!("Document still not found after repair attempt!");
                },
                Err(e) => {
                    println!("Error during repair verification: {:?}", e);
                }
            }
        },
        Err(e) => {
            println!("Error reading document: {:?}", e);
        }
    }
    
    // Scan the collection to verify IDs
    let all_ids = collection.scan()
        .map_err(TestError::from)?;
    println!("Found {} document IDs in the collection:", all_ids.len());
    for id in &all_ids {  // Use reference to avoid ownership issues
        println!("  - {}", String::from_utf8_lossy(id));
    }
    
    Ok(())
}
