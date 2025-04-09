use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use nebuladb_core::{Result, Error};
use crate::database::Database;

/// A connection to the database
#[derive(Clone)]
pub struct Connection {
    /// Database instance
    pub database: Arc<RwLock<Database>>,
    /// When this connection was created
    pub created_at: Instant,
    /// Last time this connection was used
    pub last_used: Instant,
    /// Connection ID
    pub id: u64,
    /// Whether this connection is in a transaction
    pub in_transaction: bool,
    /// Current transaction ID if in a transaction
    pub transaction_id: Option<u64>,
}

/// Connection status for monitoring
#[derive(Clone, Debug)]
pub struct ConnectionStatus {
    /// Connection ID
    pub id: u64,
    /// Database name
    pub database_name: String,
    /// Age of the connection in seconds
    pub age_secs: u64,
    /// Idle time in seconds
    pub idle_secs: u64,
    /// Whether this connection is in a transaction
    pub in_transaction: bool,
    /// Transaction ID if in a transaction
    pub transaction_id: Option<u64>,
}

/// Configuration for the connection pool
#[derive(Clone, Debug)]
pub struct ConnectionPoolConfig {
    /// Maximum number of connections
    pub max_connections: usize,
    /// Maximum number of connections per database
    pub max_connections_per_db: usize,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Idle timeout in seconds
    pub idle_timeout: u64,
    /// Transaction timeout in seconds
    pub transaction_timeout: u64,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_connections_per_db: 100,
            connection_timeout: 3600, // 1 hour
            idle_timeout: 600, // 10 minutes
            transaction_timeout: 30, // 30 seconds
        }
    }
}

/// Connection pool for database connections
pub struct ConnectionPool {
    /// Available connections by database name
    available: Mutex<HashMap<String, VecDeque<Connection>>>,
    /// In-use connections by ID
    in_use: Mutex<HashMap<u64, Connection>>,
    /// Next connection ID
    next_id: Mutex<u64>,
    /// Pool configuration
    config: ConnectionPoolConfig,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: ConnectionPoolConfig) -> Self {
        Self {
            available: Mutex::new(HashMap::new()),
            in_use: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
            config,
        }
    }
    
    /// Get a connection to a database
    pub fn get_connection(&self, database_name: &str, db: Arc<RwLock<Database>>) -> Result<Connection> {
        // First try to reuse an existing connection
        if let Some(conn) = self.get_available_connection(database_name) {
            return Ok(conn);
        }
        
        // Check if we've hit the maximum number of connections
        if self.get_connection_count() >= self.config.max_connections {
            // Try to clean up idle connections
            self.cleanup_idle_connections();
            
            // Check again
            if self.get_connection_count() >= self.config.max_connections {
                return Err(Error::Other("Connection pool full".into()));
            }
        }
        
        // Check if we've hit the maximum connections per database
        if self.get_connection_count_for_db(database_name) >= self.config.max_connections_per_db {
            return Err(Error::Other(format!(
                "Maximum connections reached for database '{}'", database_name)));
        }
        
        // Create a new connection
        let conn = self.create_connection(database_name, db)?;
        
        // Add to in-use connections
        if let Ok(mut in_use) = self.in_use.lock() {
            in_use.insert(conn.id, conn.clone());
        }
        
        Ok(conn)
    }
    
    /// Release a connection back to the pool
    pub fn release_connection(&self, conn: Connection) -> Result<()> {
        // Remove from in-use
        if let Ok(mut in_use) = self.in_use.lock() {
            in_use.remove(&conn.id);
        }
        
        // Get database name
        let db_name = if let Ok(db) = conn.database.read() {
            db.get_name().to_string()
        } else {
            return Err(Error::Other("Failed to get database name".into()));
        };
        
        // Add to available connections
        if let Ok(mut available) = self.available.lock() {
            let queue = available.entry(db_name).or_insert_with(VecDeque::new);
            queue.push_back(conn);
        }
        
        Ok(())
    }
    
    /// Clean up idle connections
    pub fn cleanup_idle_connections(&self) {
        let now = Instant::now();
        
        // Remove idle connections from the available pool
        if let Ok(mut available) = self.available.lock() {
            for (_, queue) in available.iter_mut() {
                queue.retain(|conn| {
                    let idle_secs = now.duration_since(conn.last_used).as_secs();
                    idle_secs < self.config.idle_timeout
                });
            }
        }
        
        // Check for timed out transactions
        if let Ok(mut in_use) = self.in_use.lock() {
            let mut to_abort = Vec::new();
            
            for (id, conn) in in_use.iter() {
                if conn.in_transaction {
                    let tx_duration = now.duration_since(conn.last_used).as_secs();
                    if tx_duration > self.config.transaction_timeout {
                        to_abort.push(*id);
                    }
                }
            }
            
            // Abort timed out transactions
            for id in to_abort {
                if let Some(conn) = in_use.get(&id) {
                    if let Some(tx_id) = conn.transaction_id {
                        if let Ok(mut db) = conn.database.write() {
                            let _ = db.abort_transaction(tx_id);
                        }
                    }
                }
                in_use.remove(&id);
            }
        }
    }
    
    /// Get connection status for monitoring
    pub fn get_connection_status(&self) -> Vec<ConnectionStatus> {
        let mut result = Vec::new();
        let now = Instant::now();
        
        // Add in-use connections
        if let Ok(in_use) = self.in_use.lock() {
            for conn in in_use.values() {
                if let Ok(db) = conn.database.read() {
                    result.push(ConnectionStatus {
                        id: conn.id,
                        database_name: db.get_name().to_string(),
                        age_secs: now.duration_since(conn.created_at).as_secs(),
                        idle_secs: now.duration_since(conn.last_used).as_secs(),
                        in_transaction: conn.in_transaction,
                        transaction_id: conn.transaction_id,
                    });
                }
            }
        }
        
        // Add available connections
        if let Ok(available) = self.available.lock() {
            for (db_name, queue) in available.iter() {
                for conn in queue {
                    result.push(ConnectionStatus {
                        id: conn.id,
                        database_name: db_name.clone(),
                        age_secs: now.duration_since(conn.created_at).as_secs(),
                        idle_secs: now.duration_since(conn.last_used).as_secs(),
                        in_transaction: conn.in_transaction,
                        transaction_id: conn.transaction_id,
                    });
                }
            }
        }
        
        result
    }
    
    /// Get the next connection ID
    fn get_next_id(&self) -> u64 {
        if let Ok(mut id) = self.next_id.lock() {
            *id += 1;
            *id
        } else {
            0 // Fallback
        }
    }
    
    /// Create a new connection
    fn create_connection(&self, database_name: &str, db: Arc<RwLock<Database>>) -> Result<Connection> {
        let now = Instant::now();
        let id = self.get_next_id();
        
        Ok(Connection {
            database: db,
            created_at: now,
            last_used: now,
            id,
            in_transaction: false,
            transaction_id: None,
        })
    }
    
    /// Get an available connection for the given database
    fn get_available_connection(&self, database_name: &str) -> Option<Connection> {
        if let Ok(mut available) = self.available.lock() {
            if let Some(queue) = available.get_mut(database_name) {
                if let Some(mut conn) = queue.pop_front() {
                    // Update last used
                    conn.last_used = Instant::now();
                    
                    // Add to in-use
                    if let Ok(mut in_use) = self.in_use.lock() {
                        in_use.insert(conn.id, conn.clone());
                    }
                    
                    return Some(conn);
                }
            }
        }
        
        None
    }
    
    /// Get the total number of connections
    fn get_connection_count(&self) -> usize {
        let mut count = 0;
        
        // Count in-use connections
        if let Ok(in_use) = self.in_use.lock() {
            count += in_use.len();
        }
        
        // Count available connections
        if let Ok(available) = self.available.lock() {
            for (_, queue) in available.iter() {
                count += queue.len();
            }
        }
        
        count
    }
    
    /// Get the number of connections for a database
    fn get_connection_count_for_db(&self, database_name: &str) -> usize {
        let mut count = 0;
        
        // Count in-use connections for this database
        if let Ok(in_use) = self.in_use.lock() {
            for conn in in_use.values() {
                if let Ok(db) = conn.database.read() {
                    if db.get_name() == database_name {
                        count += 1;
                    }
                }
            }
        }
        
        // Count available connections for this database
        if let Ok(available) = self.available.lock() {
            if let Some(queue) = available.get(database_name) {
                count += queue.len();
            }
        }
        
        count
    }
} 