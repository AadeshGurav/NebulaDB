use std::path::{Path, PathBuf};
use rustyline::{Editor, error::ReadlineError};
use nebuladb_core::{Result, Error};
use crate::database::Database;
use crate::util::{is_valid_json, format_output, matches_query};
use serde_json::Value as JsonValue;
use crate::interfaces::InterfaceManagerRef;
use std::sync::{Arc, RwLock};
use std::io::Write;

#[derive(Clone)]
/// CLI interface for interacting with the database
pub struct CliInterface {
    /// Reference to the database manager
    manager: InterfaceManagerRef,
    /// Command history file path
    history_path: PathBuf,
}

impl CliInterface {
    /// Create a new CLI interface
    pub fn new(manager: InterfaceManagerRef) -> Result<Self> {
        let history_path = dirs::home_dir()
            .unwrap_or_else(|| Path::new(".").to_path_buf())
            .join(".nebuladb_history");
            
        Ok(Self {
            manager,
            history_path,
        })
    }
    
    /// Start the CLI interface
    pub fn start(&mut self) -> Result<()> {
        let mut rl = Editor::<()>::new().expect("Failed to create editor");
        
        // Load history if it exists
        if rl.load_history(&self.history_path).is_err() {
            println!("No previous history found.");
        }
        
        println!("NebulaDB v0.1.0 Interactive CLI");
        println!("Type 'help' for a list of commands");
        
        // Print the active database
        if let Ok(manager) = self.manager.read() {
            if let Some(db_name) = manager.get_active_database_name() {
                println!("Current database: {}", db_name);
            }
        }
        
        loop {
            // Update the prompt to show the active database
            let prompt = if let Ok(manager) = self.manager.read() {
                match manager.get_active_database_name() {
                    Some(name) => format!("nebuladb:{}> ", name),
                    None => "nebuladb> ".to_string(),
                }
            } else {
                "nebuladb> ".to_string()
            };
            
            match rl.readline(&prompt) {
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
                        "help" => self.show_help(),
                        
                        // Database commands
                        "createdb" => self.create_database(&parts),
                        "usedb" => self.use_database(&parts),
                        "listdb" => self.list_databases(),
                        "dropdb" => self.drop_database(&parts),
                        
                        // Collection commands
                        "list" => self.list_collections(),
                        "open" => self.open_collection(&parts),
                        "close" => self.close_collection(&parts),
                        "create" => self.create_collection(&parts),
                        
                        // Document commands
                        "insert" => self.insert_document(&parts),
                        "json" => self.insert_json_document(&parts),
                        "get" => self.get_document(&parts),
                        "delete" => self.delete_document(&parts),
                        "scan" => self.scan_collection(&parts),
                        "find" => self.find_documents(&parts),
                        
                        // System commands
                        "clear" => self.clear_screen(),
                        "exit" | "quit" => {
                            println!("Exiting NebulaDB. Goodbye!");
                            break;
                        },
                        _ => println!("Unknown command: {}. Type 'help' for a list of commands", command),
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
        
        // Save command history
        if let Err(e) = rl.save_history(&self.history_path) {
            eprintln!("Error saving history: {}", e);
        }
        
        Ok(())
    }
    
    /// Show help message
    fn show_help(&self) {
        println!("Available commands:");
        println!("  Database commands:");
        println!("  createdb <name>                     - Create a new database");
        println!("  usedb <name>                        - Switch to a database");
        println!("  listdb                              - List all databases");
        println!("  dropdb <name>                       - Delete a database");
        println!();
        println!("  Collection commands:");
        println!("  list                                - List all collections (both open and on disk)");
        println!("  open <collection_name>              - Open or create a collection");
        println!("  close <collection_name>             - Close a collection");
        println!("  create <collection_name>            - Create a new collection");
        println!();
        println!("  Document commands:");
        println!("  insert <collection> <id> <data>     - Insert a document");
        println!("  json <collection> <id> <json>       - Insert a JSON document");
        println!("  get <collection> <id>               - Get a document");
        println!("  delete <collection> <id>            - Delete a document");
        println!("  scan <collection>                   - List all documents in a collection");
        println!("  find <collection> [query]           - Find documents in a collection");
        println!();
        println!("  System commands:");
        println!("  clear                               - Clear the terminal screen");
        println!("  exit                                - Exit the program");
    }
    
    /// Create a new database
    fn create_database(&mut self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: createdb <n>");
            return;
        }
        
        let name = parts[1];
        
        if let Ok(mut manager) = self.manager.write() {
            match manager.create_database(name) {
                Ok(_) => println!("Database '{}' created successfully", name),
                Err(e) => println!("Error creating database '{}': {:?}", name, e),
            }
        } else {
            println!("Error: could not access database manager");
        }
    }
    
    /// Switch to a different database
    fn use_database(&mut self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: usedb <n>");
            return;
        }
        
        let name = parts[1];
        
        if let Ok(mut manager) = self.manager.write() {
            match manager.set_active_database(name) {
                Ok(_) => println!("Switched to database '{}'", name),
                Err(e) => println!("Error switching to database '{}': {:?}", name, e),
            }
        } else {
            println!("Error: could not access database manager");
        }
    }
    
    /// List all databases
    fn list_databases(&self) {
        if let Ok(manager) = self.manager.read() {
            let databases = manager.list_databases();
            let active_db = manager.get_active_database_name();
            
            if databases.is_empty() {
                println!("No databases found");
                return;
            }
            
            println!("Databases:");
            for name in &databases {
                let status = if Some(name.clone()) == active_db {
                    "(active)"
                } else {
                    ""
                };
                println!("  - {} {}", name, status);
            }
            println!("Total: {} databases", databases.len());
        } else {
            println!("Error: could not access database manager");
        }
    }
    
    /// Drop (delete) a database
    fn drop_database(&mut self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: dropdb <n>");
            return;
        }
        
        let name = parts[1];
        
        // Confirm deletion
        println!("Are you sure you want to delete database '{}'? [y/N]", name);
        let mut input = String::new();
        if let Err(e) = std::io::stdin().read_line(&mut input) {
            println!("Error reading input: {:?}", e);
            return;
        }
        
        if input.trim().to_lowercase() != "y" {
            println!("Operation cancelled");
            return;
        }
        
        if let Ok(mut manager) = self.manager.write() {
            match manager.drop_database(name) {
                Ok(_) => println!("Database '{}' deleted successfully", name),
                Err(e) => println!("Error deleting database '{}': {:?}", name, e),
            }
        } else {
            println!("Error: could not access database manager");
        }
    }
    
    /// Get a reference to the active database
    fn get_active_db(&self) -> Result<Arc<RwLock<Database>>> {
        let manager = self.manager.read()
            .map_err(|_| Error::Other("Failed to lock interface manager".into()))?;
        manager.get_active_database()
    }
    
    /// List all collections
    fn list_collections(&self) {
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                let all_collections = db.list_collections();
                let open_collections = db.list_open_collections();
                
                if all_collections.is_empty() {
                    println!("No collections found");
                    return;
                }
                
                println!("Collections:");
                for name in &all_collections {
                    let status = if open_collections.contains(name) {
                        "open"
                    } else {
                        "closed"
                    };
                    println!("  - {} ({})", name, status);
                }
                println!("Total: {} collections ({} open)", 
                    all_collections.len(), 
                    open_collections.len()
                );
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Open a collection
    fn open_collection(&mut self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: open <collection_name>");
            return;
        }
        
        let name = parts[1];
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let mut db = db_rwlock.write().unwrap();
                let open_collections = db.list_open_collections();
                
                if open_collections.contains(&name.to_string()) {
                    println!("Collection '{}' is already open", name);
                    return;
                }
                
                match db.open_collection(name) {
                    Ok(_) => println!("Collection '{}' opened successfully", name),
                    Err(e) => println!("Error opening collection '{}': {:?}", name, e),
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Close a collection
    fn close_collection(&mut self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: close <collection_name>");
            return;
        }
        
        let name = parts[1];
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let mut db = db_rwlock.write().unwrap();
                match db.close_collection(name) {
                    Ok(_) => println!("Collection '{}' closed successfully", name),
                    Err(e) => println!("Error closing collection '{}': {:?}", name, e),
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Create a new collection without opening it
    fn create_collection(&self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: create <collection_name>");
            return;
        }
        
        let name = parts[1];
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                
                // Check if collection already exists
                if db.collection_exists(name) {
                    println!("Collection '{}' already exists", name);
                    return;
                }
                
                // We need a write lock to create the collection
                drop(db);
                
                let mut db = db_rwlock.write().unwrap();
                match db.create_collection(name) {
                    Ok(_) => println!("Collection '{}' created successfully", name),
                    Err(e) => println!("Error creating collection '{}': {:?}", name, e),
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Insert a document
    fn insert_document(&mut self, parts: &[&str]) {
        if parts.len() < 4 {
            println!("Usage: insert <collection> <id> <data>");
            return;
        }
        
        let collection_name = parts[1];
        let id = parts[2].as_bytes();
        let data = parts[3..].join(" ").as_bytes().to_vec();
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(mut collection) = collection_mutex.lock() {
                        match collection.insert(id, &data) {
                            Ok(_) => println!("Document inserted successfully"),
                            Err(e) => println!("Error inserting document: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Insert a JSON document
    fn insert_json_document(&mut self, parts: &[&str]) {
        if parts.len() < 4 {
            println!("Usage: json <collection> <id> <json_data>");
            println!("Example: json users user123 {{\"name\":\"John\",\"age\":30}}");
            return;
        }
        
        let collection_name = parts[1];
        let id = parts[2].as_bytes();
        
        // Join the rest as the JSON string
        let json_str = parts[3..].join(" ");
        
        // Validate JSON
        if !is_valid_json(&json_str) {
            println!("Error: Invalid JSON data");
            return;
        }
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(mut collection) = collection_mutex.lock() {
                        match collection.insert(id, json_str.as_bytes()) {
                            Ok(_) => println!("JSON document inserted successfully"),
                            Err(e) => println!("Error inserting document: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Get a document
    fn get_document(&self, parts: &[&str]) {
        if parts.len() < 3 {
            println!("Usage: get <collection> <id>");
            return;
        }
        
        let collection_name = parts[1];
        let id = parts[2].as_bytes();
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(collection) = collection_mutex.lock() {
                        match collection.get(id) {
                            Ok(Some(data)) => {
                                let data_str = String::from_utf8_lossy(&data);
                                format_output(&data_str);
                            },
                            Ok(None) => println!("Document not found"),
                            Err(e) => println!("Error retrieving document: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Delete a document
    fn delete_document(&mut self, parts: &[&str]) {
        if parts.len() < 3 {
            println!("Usage: delete <collection> <id>");
            return;
        }
        
        let collection_name = parts[1];
        let id = parts[2].as_bytes();
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(mut collection) = collection_mutex.lock() {
                        match collection.delete(id) {
                            Ok(true) => println!("Document deleted successfully"),
                            Ok(false) => println!("Document not found"),
                            Err(e) => println!("Error deleting document: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
    
    /// Scan a collection
    fn scan_collection(&self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: scan <collection>");
            return;
        }
        
        let collection_name = parts[1];
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(collection) = collection_mutex.lock() {
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
                            Err(e) => println!("Error scanning collection: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }

    /// Find documents in a collection
    fn find_documents(&self, parts: &[&str]) {
        if parts.len() < 2 {
            println!("Usage: find <collection> [query]");
            println!("Examples:");
            println!("  find users                     - Get all documents");
            println!("  find users {{\"name\":\"John\"}}    - Find documents where name = John");
            return;
        }
        
        let collection_name = parts[1];
        
        // Parse query if provided
        let query_str = if parts.len() > 2 {
            parts[2..].join(" ")
        } else {
            "{}".to_string() // Empty query matches all documents
        };
        
        println!("DEBUG: Using query string: '{}'", query_str);
        
        // Validate JSON
        let query = match serde_json::from_str::<JsonValue>(&query_str) {
            Ok(q) => q,
            Err(e) => {
                println!("Invalid JSON query: {}", e);
                return;
            }
        };
        
        println!("DEBUG: Parsed query: {:?}", query);
        
        match self.get_active_db() {
            Ok(db_rwlock) => {
                let db = db_rwlock.read().unwrap();
                if let Some(collection_mutex) = db.get_collection(collection_name) {
                    // Lock the collection to access it
                    if let Ok(collection) = collection_mutex.lock() {
                        // Get all document IDs
                        match collection.scan() {
                            Ok(ids) => {
                                if ids.is_empty() {
                                    println!("No documents found in collection '{}'", collection_name);
                                    return;
                                }
                                
                                println!("DEBUG: Found {} document IDs", ids.len());
                                
                                let mut found_count = 0;
                                
                                // For each ID, get the document and check if it matches the query
                                for id in &ids {
                                    println!("DEBUG: Checking document with ID: {}", String::from_utf8_lossy(id));
                                    match collection.get(&id) {
                                        Ok(Some(data)) => {
                                            let doc_str = String::from_utf8_lossy(&data);
                                            println!("DEBUG: Document content: {}", doc_str);
                                            
                                            if matches_query(&doc_str, &query) {
                                                println!("DEBUG: Document matches query!");
                                                found_count += 1;
                                                println!("ID: {}", String::from_utf8_lossy(id));
                                                format_output(&doc_str);
                                                println!("---");
                                            } else {
                                                println!("DEBUG: Document does NOT match query");
                                            }
                                        },
                                        Ok(None) => println!("DEBUG: Document with ID {} not found", String::from_utf8_lossy(id)),
                                        Err(e) => println!("DEBUG: Error retrieving document: {:?}", e),
                                    }
                                }
                                
                                if found_count == 0 {
                                    println!("No documents matched the query");
                                } else {
                                    println!("Found {} matching document(s)", found_count);
                                }
                            },
                            Err(e) => println!("Error scanning collection: {:?}", e),
                        }
                    } else {
                        println!("Failed to lock collection");
                    }
                } else {
                    println!("Collection '{}' is not open", collection_name);
                }
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }

    /// Clear the terminal screen
    fn clear_screen(&self) {
        if cfg!(target_os = "windows") {
            // For Windows
            if std::process::Command::new("cmd")
                .args(["/C", "cls"])
                .status()
                .is_err()
            {
                // Fallback to ANSI escape codes
                print!("\x1B[2J\x1B[1;1H");
            }
        } else {
            // For Unix-like systems
            if std::process::Command::new("clear")
                .status()
                .is_err()
            {
                // Fallback to ANSI escape codes
                print!("\x1B[2J\x1B[1;1H");
            }
        }
        // Ensure output is flushed
        if let Err(e) = std::io::stdout().flush() {
            eprintln!("Error flushing stdout: {}", e);
        }
    }
}
