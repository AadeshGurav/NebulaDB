use nebuladb_core::{Result, Error};
use nebuladb_storage::{
    StorageConfig,
    collection::Collection
};
use std::path::Path;

fn main() -> Result<()> {
    println!("NebulaDB v0.1.0");
    println!("Initializing storage engine...");
    
    // Create a data directory
    let data_dir = Path::new("./data");
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir).map_err(|e| Error::IoError(e))?;
    }
    
    // Create storage config
    let config = StorageConfig::default();
    
    // Create a test collection directly
    println!("Creating a test collection...");
    let collection_name = "test_collection";
    let mut collection = Collection::open(collection_name, data_dir, &config)?;
    
    // Insert a test document
    let doc_id = b"test_doc_1";
    let doc_data = b"{\"name\": \"Test Document\", \"value\": 42}";
    collection.insert(doc_id, doc_data)?;
    println!("Inserted test document with ID: {:?}", String::from_utf8_lossy(doc_id));
    
    // Close the collection
    println!("Closing collection...");
    collection.close()?;
    
    println!("NebulaDB successfully initialized and test document inserted.");
    println!("Check the ./data directory for the created files.");
    
    Ok(())
} 