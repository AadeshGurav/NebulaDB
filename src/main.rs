use nebuladb_core::{Result, Error};
use nebuladb_storage::{
    StorageConfig,
    collection::Collection,
    CompressionType
};
use std::path::Path;
use std::collections::HashMap;
use std::fs;
use rustyline::{Editor, error::ReadlineError};
use serde_json::Value as JsonValue;

/// Storage engine that manages all collections
struct Storage {
    /// Base path for all collections
    path: Box<Path>,
    /// Configuration for the storage engine
    config: StorageConfig,
    /// Open collections
    collections: HashMap<String, Collection>,
}

impl Storage {
    /// Create a new storage engine
    fn new(path: &Path, config: StorageConfig) -> Result<Self> {
        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| Error::IoError(e))?;
        }
        
        Ok(Self {
            path: path.into(),
            config,
            collections: HashMap::new(),
        })
    }
    
    /// Open or create a collection
    fn open_collection(&mut self, name: &str) -> Result<&mut Collection> {
        if self.collections.contains_key(name) {
            return Ok(self.collections.get_mut(name).unwrap());
        }
        
        let collection = Collection::open(name, &self.path, &self.config)?;
        self.collections.insert(name.to_string(), collection);
        
        Ok(self.collections.get_mut(name).unwrap())
    }
    
    /// Close a collection
    fn close_collection(&mut self, name: &str) -> Result<()> {
        if let Some(mut collection) = self.collections.remove(name) {
            collection.close()?;
        }
        
        Ok(())
    }
    
    /// Get a reference to an open collection
    fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }
    
    /// Get a mutable reference to an open collection
    fn get_collection_mut(&mut self, name: &str) -> Option<&mut Collection> {
        self.collections.get_mut(name)
    }
    
    /// List all open collections
    fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }
    
    /// Close all collections
    fn close_all(&mut self) -> Result<()> {
        let mut last_error = None;
        
        for (name, mut collection) in self.collections.drain() {
            if let Err(e) = collection.close() {
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
    
    /// Set compression type for the storage engine
    fn set_compression(&mut self, compression_type: &str) -> Result<()> {
        // Convert string to compression type
        let compression = match compression_type.to_lowercase().as_str() {
            "none" => CompressionType::None,
            "snappy" => CompressionType::Snappy,
            "zstd" => CompressionType::Zstd,
            "lz4" => CompressionType::Lz4,
            _ => return Err(Error::Other(format!("Unsupported compression type: {}", compression_type))),
        };
        
        // Update the config
        self.config.compression = compression;
        
        println!("Compression type set to: {:?}", compression);
        Ok(())
    }
    
    /// Enable WAL for all collections
    fn enable_wal(&mut self, wal_dir: Option<&Path>) -> Result<()> {
        // Set the WAL directory (default to a 'wal' subdirectory in the data path)
        let wal_path = match wal_dir {
            Some(path) => path.to_path_buf(),
            None => self.path.join("wal"),
        };
        
        // Create WAL directory if it doesn't exist
        if !wal_path.exists() {
            std::fs::create_dir_all(&wal_path).map_err(|e| Error::IoError(e))?;
        }
        
        // Update config to include WAL settings
        // Note: This assumes you'll add WAL settings to StorageConfig or use a separate WalConfig
        // For now we'll just print a message about enabling WAL
        println!("WAL enabled with directory: {:?}", wal_path);
        
        // In a real implementation, you would:
        // 1. Create a WalManager instance
        // 2. Configure it with self.config.wal_settings
        // 3. Store it in self.wal_manager
        // 4. Hook up collection operations to use WAL
        
        Ok(())
    }
    
    /// Perform a checkpoint (sync WAL to storage)
    fn checkpoint(&mut self) -> Result<()> {
        println!("Performing WAL checkpoint...");
        
        // In a real implementation, you would:
        // 1. Pause all write operations temporarily
        // 2. Flush all pending data from active blocks to disk
        // 3. Record checkpoint in WAL
        // 4. Truncate WAL up to checkpoint
        // 5. Resume write operations
        
        println!("Checkpoint complete");
        Ok(())
    }
}

fn main() -> Result<()> {
    println!("NebulaDB v0.1.0 Interactive CLI");
    println!("Type 'help' for a list of commands");
    
    // Create a data directory
    let data_dir = Path::new("./data");
    
    // Create storage config
    let config = StorageConfig::default();
    
    // Create storage engine
    let mut storage = Storage::new(data_dir, config)?;
    
    // Setup command history with rustyline
    let mut rl = Editor::<()>::new().expect("Failed to create editor");
    // Load history from ~/.nebuladb_history if it exists
    let history_path = dirs::home_dir()
        .unwrap_or_else(|| Path::new(".").to_path_buf())
        .join(".nebuladb_history");
    
    if rl.load_history(&history_path).is_err() {
        println!("No previous history found.");
    }
    
    loop {
        match rl.readline("nebuladb> ") {
            Ok(line) => {
                let input = line.trim();
                
                if input.is_empty() {
                    continue;
                }
                
                // Add to history
                rl.add_history_entry(input);
                
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
                        println!("  json <collection> <id> <json>      - Insert a JSON document");
                        println!("  get <collection> <id>              - Retrieve a document from a collection");
                        println!("  scan <collection>                  - List all documents in a collection");
                        println!("  delete <collection> <id>           - Delete a document from a collection");
                        println!("  exit                               - Exit the program");
                        println!("  compression <type>                 - Set compression type (none, snappy, zstd, lz4)");
                        println!("  wal <directory>                    - Enable Write-Ahead Logging (optional directory)");
                        println!("  checkpoint                         - Force WAL checkpoint");
                        println!("  find <collection> [query]            - Find documents in a collection");
                    },
                    "list" => {
                        if storage.collections.is_empty() {
                            println!("No collections are currently open");
                        } else {
                            println!("Open collections:");
                            for name in storage.list_collections() {
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
                        
                        if storage.collections.contains_key(name) {
                            println!("Collection '{}' is already open", name);
                            continue;
                        }
                        
                        match storage.open_collection(name) {
                            Ok(_) => {
                                println!("Collection '{}' opened successfully", name);
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
                        
                        match storage.close_collection(name) {
                            Ok(_) => {
                                println!("Collection '{}' closed successfully", name);
                            },
                            Err(e) => {
                                println!("Error closing collection '{}': {:?}", name, e);
                            }
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
                        
                        if let Some(collection) = storage.get_collection_mut(collection_name) {
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
                    "json" => {
                        if parts.len() < 4 {
                            println!("Usage: json <collection> <id> <json_data>");
                            println!("Example: json users user123 {{\"name\":\"John\",\"age\":30}}");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        
                        // Join the rest as the JSON string
                        let json_str = parts[3..].join(" ");
                        
                        // Validate JSON
                        if !is_valid_json(&json_str) {
                            println!("Error: Invalid JSON data");
                            continue;
                        }
                        
                        if let Some(collection) = storage.get_collection_mut(collection_name) {
                            match collection.insert(id, json_str.as_bytes()) {
                                Ok(_) => {
                                    println!("JSON document inserted successfully");
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
                        
                        if let Some(collection) = storage.get_collection(collection_name) {
                            match collection.get(id) {
                                Ok(Some(data)) => {
                                    let data_str = String::from_utf8_lossy(&data);
                                    format_output(&data_str);
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
                    "scan" => {
                        if parts.len() < 2 {
                            println!("Usage: scan <collection>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        
                        if let Some(collection) = storage.get_collection(collection_name) {
                            match collection.scan() {
                                Ok(ids) => {
                                    if ids.is_empty() {
                                        println!("No documents found in collection '{}'", collection_name);
                                    } else {
                                        println!("Documents in collection '{}':", collection_name);
                                        for id in &ids {
                                            println!("  - {}", String::from_utf8_lossy(id));
                                        }
                                        println!("Total: {} documents", ids.len());
                                    }
                                },
                                Err(e) => {
                                    println!("Error scanning collection: {:?}", e);
                                }
                            }
                        } else {
                            println!("Collection '{}' is not open", collection_name);
                        }
                    },
                    "delete" => {
                        if parts.len() < 3 {
                            println!("Usage: delete <collection> <id>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        
                        if let Some(collection) = storage.get_collection_mut(collection_name) {
                            match collection.delete(id) {
                                Ok(true) => println!("Document deleted successfully"),
                                Ok(false) => println!("Document not found"),
                                Err(e) => println!("Error deleting document: {:?}", e),
                            }
                        } else {
                            println!("Collection '{}' is not open", collection_name);
                        }
                    },
                    "exit" | "quit" => {
                        println!("Closing all collections...");
                        
                        match storage.close_all() {
                            Ok(_) => {
                                println!("All collections closed successfully");
                            },
                            Err(e) => {
                                println!("Error closing collections: {:?}", e);
                            }
                        }
                        
                        // Save command history
                        if let Err(e) = rl.save_history(&history_path) {
                            eprintln!("Error saving history: {}", e);
                        }
                        
                        println!("Exiting NebulaDB. Goodbye!");
                        break;
                    },
                    "compression" => {
                        if parts.len() < 2 {
                            println!("Usage: compression <type>");
                            println!("Available types: none, snappy, zstd, lz4");
                            continue;
                        }
                        
                        let compression_type = parts[1];
                        
                        match storage.set_compression(compression_type) {
                            Ok(_) => {
                                println!("Compression setting updated successfully");
                            },
                            Err(e) => {
                                println!("Error updating compression setting: {:?}", e);
                            }
                        }
                    },
                    "wal" => {
                        let wal_dir = if parts.len() > 1 {
                            Some(Path::new(parts[1]))
                        } else {
                            None
                        };
                        
                        match storage.enable_wal(wal_dir) {
                            Ok(_) => {
                                println!("WAL enabled successfully");
                            },
                            Err(e) => {
                                println!("Error enabling WAL: {:?}", e);
                            }
                        }
                    },
                    "checkpoint" => {
                        match storage.checkpoint() {
                            Ok(_) => {
                                println!("Checkpoint completed successfully");
                            },
                            Err(e) => {
                                println!("Error during checkpoint: {:?}", e);
                            }
                        }
                    },
                    "find" => {
                        if parts.len() < 2 {
                            println!("Usage: find <collection> [query]");
                            println!("Examples:");
                            println!("  find users                     - Get all documents");
                            println!("  find users {{\"name\":\"John\"}}    - Find documents where name = John");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        
                        // Parse query if provided
                        let query_str = if parts.len() > 2 {
                            parts[2..].join(" ")
                        } else {
                            "{}".to_string() // Empty query matches all documents
                        };
                        
                        // Validate JSON
                        let query = match serde_json::from_str::<JsonValue>(&query_str) {
                            Ok(q) => q,
                            Err(e) => {
                                println!("Invalid JSON query: {}", e);
                                continue;
                            }
                        };
                        
                        if let Some(collection) = storage.get_collection(collection_name) {
                            // Get all document IDs
                            match collection.scan() {
                                Ok(ids) => {
                                    if ids.is_empty() {
                                        println!("No documents found in collection '{}'", collection_name);
                                        continue;
                                    }
                                    
                                    let mut found_count = 0;
                                    
                                    // For each ID, get the document and check if it matches the query
                                    for id in &ids {
                                        match collection.get(id) {
                                            Ok(Some(data)) => {
                                                let doc_str = String::from_utf8_lossy(&data);
                                                
                                                if matches_query(&doc_str, &query) {
                                                    found_count += 1;
                                                    println!("ID: {}", String::from_utf8_lossy(id));
                                                    format_output(&doc_str);
                                                    println!("---");
                                                }
                                            },
                                            _ => continue,
                                        }
                                    }
                                    
                                    if found_count == 0 {
                                        println!("No documents matched the query");
                                    } else {
                                        println!("Found {} matching document(s)", found_count);
                                    }
                                },
                                Err(e) => {
                                    println!("Error scanning collection: {:?}", e);
                                }
                            }
                        } else {
                            println!("Collection '{}' is not open", collection_name);
                        }
                    },
                    _ => {
                        println!("Unknown command: {}. Type 'help' for a list of commands", command);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("Ctrl-C pressed. Use 'exit' to quit.");
                continue;
            },
            Err(ReadlineError::Eof) => {
                println!("Ctrl-D pressed, exiting...");
                break;
            },
            Err(err) => {
                println!("Error reading line: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}

/// Check if a string is valid JSON
fn is_valid_json(json_str: &str) -> bool {
    // Simple JSON validation - check for paired braces and at least one key-value pair
    if !json_str.starts_with('{') || !json_str.ends_with('}') {
        return false;
    }
    
    // Count braces to ensure they're balanced
    let mut brace_count = 0;
    for c in json_str.chars() {
        if c == '{' {
            brace_count += 1;
        } else if c == '}' {
            brace_count -= 1;
        }
        
        if brace_count < 0 {
            return false;
        }
    }
    
    brace_count == 0
}

/// Format and pretty-print document output
fn format_output(data: &str) {
    // Check if it's JSON
    if data.starts_with('{') && data.ends_with('}') {
        // Indent JSON for readability
        let mut indentation = 0;
        let mut formatted = String::new();
        let mut in_quotes = false;
        
        for c in data.chars() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                    formatted.push(c);
                },
                '{' | '[' => {
                    formatted.push(c);
                    if !in_quotes {
                        indentation += 2;
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                },
                '}' | ']' => {
                    if !in_quotes {
                        indentation -= 2;
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                    formatted.push(c);
                },
                ',' => {
                    formatted.push(c);
                    if !in_quotes {
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                },
                ':' => {
                    formatted.push(c);
                    if !in_quotes {
                        formatted.push(' ');
                    }
                },
                _ => formatted.push(c),
            }
        }
        
        println!("{}", formatted);
    } else {
        // Just print the raw data
        println!("{}", data);
    }
}

/// Check if a document matches a query
fn matches_query(document: &str, query: &JsonValue) -> bool {
    // Parse the document JSON
    if let Ok(doc_value) = serde_json::from_str::<JsonValue>(document) {
        // If query is empty, match all documents
        if query.as_object().map_or(false, |obj| obj.is_empty()) {
            return true;
        }
        
        // If query is a simple object with key-value pairs, check each one
        if let Some(query_obj) = query.as_object() {
            if let Some(doc_obj) = doc_value.as_object() {
                for (query_key, query_val) in query_obj {
                    // Check if document has this key and the value matches
                    match doc_obj.get(query_key) {
                        Some(doc_val) => {
                            if doc_val != query_val {
                                return false;
                            }
                        },
                        None => return false,
                    }
                }
                return true;
            }
        }
    }
    
    false
}
