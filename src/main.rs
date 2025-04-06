use nebuladb_core::{Result, Error};
use nebuladb_storage::{
    StorageConfig,
    collection::Collection
};
use std::path::Path;
use std::io::{self, Write, BufRead};
use std::collections::HashMap;

fn main() -> Result<()> {
    println!("NebulaDB v0.1.0 Interactive CLI");
    println!("Type 'help' for a list of commands");
    
    // Create a data directory
    let data_dir = Path::new("./data");
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir).map_err(|e| Error::IoError(e))?;
    }
    
    // Create storage config
    let config = StorageConfig::default();
    
    // Store open collections
    let mut collections: HashMap<String, Collection> = HashMap::new();
    
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    
    loop {
        print!("nebuladb> ");
        stdout.flush().unwrap();
        
        let mut input = String::new();
        stdin.lock().read_line(&mut input).unwrap();
        let input = input.trim();
        
        if input.is_empty() {
            continue;
        }
        
        let parts: Vec<&str> = input.split_whitespace().collect();
        let command = parts[0].to_lowercase();
        
        match command.as_str() {
            "help" => {
                println!("Available commands:");
                println!("  help                               - Show this help message");
                println!("  list                               - List all open collections");
                println!("  open <collection_name>             - Open or create a collection");
                println!("  close <collection_name>            - Close a collection");
                println!("  insert <collection> <id> <data>    - Insert a document into a collection");
                println!("  get <collection> <id>              - Retrieve a document from a collection");
                println!("  delete <collection> <id>           - Delete a document from a collection (not implemented yet)");
                println!("  exit                               - Exit the program");
            },
            "list" => {
                if collections.is_empty() {
                    println!("No collections are currently open");
                } else {
                    println!("Open collections:");
                    for name in collections.keys() {
                        println!("  - {}", name);
                    }
                }
            },
            "open" => {
                if parts.len() < 2 {
                    println!("Usage: open <collection_name>");
                    continue;
                }
                
                let name = parts[1];
                
                if collections.contains_key(name) {
                    println!("Collection '{}' is already open", name);
                    continue;
                }
                
                match Collection::open(name, data_dir, &config) {
                    Ok(collection) => {
                        println!("Collection '{}' opened successfully", name);
                        collections.insert(name.to_string(), collection);
                    },
                    Err(e) => {
                        println!("Error opening collection '{}': {:?}", name, e);
                    }
                }
            },
            "close" => {
                if parts.len() < 2 {
                    println!("Usage: close <collection_name>");
                    continue;
                }
                
                let name = parts[1];
                
                if let Some(mut collection) = collections.remove(name) {
                    match collection.close() {
                        Ok(_) => {
                            println!("Collection '{}' closed successfully", name);
                        },
                        Err(e) => {
                            println!("Error closing collection '{}': {:?}", name, e);
                            // Put it back in the map
                            collections.insert(name.to_string(), collection);
                        }
                    }
                } else {
                    println!("Collection '{}' is not open", name);
                }
            },
            "insert" => {
                if parts.len() < 4 {
                    println!("Usage: insert <collection> <id> <data>");
                    continue;
                }
                
                let collection_name = parts[1];
                let id = parts[2].as_bytes();
                // Join the rest of the parts to form the data
                let data = parts[3..].join(" ").as_bytes().to_vec();
                
                if let Some(collection) = collections.get_mut(collection_name) {
                    match collection.insert(id, &data) {
                        Ok(_) => {
                            println!("Document inserted successfully");
                        },
                        Err(e) => {
                            println!("Error inserting document: {:?}", e);
                        }
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            "get" => {
                if parts.len() < 3 {
                    println!("Usage: get <collection> <id>");
                    continue;
                }
                
                let collection_name = parts[1];
                let id = parts[2].as_bytes();
                
                if let Some(collection) = collections.get(collection_name) {
                    match collection.get(id) {
                        Ok(Some(data)) => {
                            println!("Document found: {}", String::from_utf8_lossy(&data));
                        },
                        Ok(None) => {
                            println!("Document not found");
                        },
                        Err(e) => {
                            println!("Error retrieving document: {:?}", e);
                        }
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            "delete" => {
                println!("Delete functionality is not yet implemented");
            },
            "exit" | "quit" => {
                println!("Closing all collections...");
                
                for (name, mut collection) in collections.drain() {
                    match collection.close() {
                        Ok(_) => {
                            println!("Collection '{}' closed successfully", name);
                        },
                        Err(e) => {
                            println!("Error closing collection '{}': {:?}", name, e);
                        }
                    }
                }
                
                println!("Exiting NebulaDB. Goodbye!");
                break;
            },
            _ => {
                println!("Unknown command: {}. Type 'help' for a list of commands", command);
            }
        }
    }
    
    Ok(())
}
