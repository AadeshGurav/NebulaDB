//! WAL log file operations
//!
//! This module handles the low-level operations on WAL log files.

use crate::entry::WalEntry;
use crate::error::{WalError, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// WAL log file format version
const WAL_FORMAT_VERSION: u8 = 1;

/// WAL log file header size in bytes
const WAL_HEADER_SIZE: usize = 16;

/// WAL log file magic bytes: "NBWA"
const WAL_MAGIC: [u8; 4] = [0x4E, 0x42, 0x57, 0x41];

/// A Write-Ahead Log file
pub struct WalLog {
    /// Path to the WAL file
    path: PathBuf,
    /// Open file handle
    file: File,
    /// Current position in the file
    position: u64,
    /// Whether to sync after every write
    sync_on_write: bool,
}

impl WalLog {
    /// Create a new WAL log file
    pub fn create(path: impl AsRef<Path>, sync_on_write: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| WalError::Io(e))?;
        }
        
        // Open the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| WalError::Io(e))?;
        
        // Write WAL header
        // Format: [magic(4)][version(1)][reserved(3)][timestamp(8)]
        file.write_all(&WAL_MAGIC).map_err(|e| WalError::Io(e))?;
        file.write_all(&[WAL_FORMAT_VERSION]).map_err(|e| WalError::Io(e))?;
        file.write_all(&[0, 0, 0]).map_err(|e| WalError::Io(e))?; // Reserved
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        file.write_all(&timestamp.to_le_bytes()).map_err(|e| WalError::Io(e))?;
        
        if sync_on_write {
            file.sync_all().map_err(|e| WalError::Io(e))?;
        }
        
        Ok(Self {
            path,
            file,
            position: WAL_HEADER_SIZE as u64,
            sync_on_write,
        })
    }
    
    /// Open an existing WAL log file
    pub fn open(path: impl AsRef<Path>, sync_on_write: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Open the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .map_err(|e| WalError::Io(e))?;
        
        // Read and verify WAL header
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(|e| WalError::Io(e))?;
        
        if magic != WAL_MAGIC {
            return Err(WalError::Other("Invalid WAL file: wrong magic number".to_string()));
        }
        
        let mut version = [0u8; 1];
        file.read_exact(&mut version).map_err(|e| WalError::Io(e))?;
        
        if version[0] != WAL_FORMAT_VERSION {
            return Err(WalError::Other(format!("Unsupported WAL format version: {}", version[0])));
        }
        
        // Skip reserved bytes
        file.seek(SeekFrom::Current(3)).map_err(|e| WalError::Io(e))?;
        
        // Skip timestamp
        file.seek(SeekFrom::Current(8)).map_err(|e| WalError::Io(e))?;
        
        // Get the current file size
        let position = file.seek(SeekFrom::End(0)).map_err(|e| WalError::Io(e))?;
        
        Ok(Self {
            path,
            file,
            position,
            sync_on_write,
        })
    }
    
    /// Append an entry to the WAL
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        // Seek to the end
        self.file.seek(SeekFrom::Start(self.position))
            .map_err(|e| WalError::Io(e))?;
        
        // Write the entry
        let entry_bytes = entry.to_bytes();
        let entry_pos = self.position;
        
        self.file.write_all(&entry_bytes).map_err(|e| WalError::Io(e))?;
        
        // Update position
        self.position += entry_bytes.len() as u64;
        
        // Sync if needed
        if self.sync_on_write {
            self.file.sync_data().map_err(|e| WalError::Io(e))?;
        }
        
        Ok(entry_pos)
    }
    
    /// Force sync the WAL to disk
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data().map_err(|e| WalError::Io(e))?;
        Ok(())
    }
    
    /// Read an entry at the given position
    pub fn read_at(&mut self, position: u64) -> Result<WalEntry> {
        if position < WAL_HEADER_SIZE as u64 || position >= self.position {
            return Err(WalError::Other(format!("Invalid WAL position: {}", position)));
        }
        
        // Seek to the position
        self.file.seek(SeekFrom::Start(position))
            .map_err(|e| WalError::Io(e))?;
        
        // Read a buffer (start with 4KB, which should be enough for most entries)
        let mut buffer = vec![0u8; 4096];
        let bytes_read = self.file.read(&mut buffer)
            .map_err(|e| WalError::Io(e))?;
        
        if bytes_read == 0 {
            return Err(WalError::Other("Unexpected end of WAL file".to_string()));
        }
        
        buffer.truncate(bytes_read);
        
        // Parse the entry
        let (entry, _) = WalEntry::from_bytes(&buffer)?;
        
        Ok(entry)
    }
    
    /// Iterate through all entries in the WAL
    pub fn iterate(&mut self) -> Result<WalIterator> {
        // Seek to the beginning (after header)
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))
            .map_err(|e| WalError::Io(e))?;
        
        Ok(WalIterator {
            file: &mut self.file,
            position: WAL_HEADER_SIZE as u64,
            end_position: self.position,
            buffer: vec![0u8; 4096], // Start with 4KB buffer
        })
    }
    
    /// Get the current size of the WAL file
    pub fn size(&self) -> u64 {
        self.position
    }
    
    /// Check if the WAL file is empty (only contains header)
    pub fn is_empty(&self) -> bool {
        self.position <= WAL_HEADER_SIZE as u64
    }
    
    /// Get the path to the WAL file
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    /// Close the WAL file
    pub fn close(self) -> Result<()> {
        self.file.sync_all().map_err(|e| WalError::Io(e))?;
        Ok(())
    }
}

/// Iterator over WAL entries
pub struct WalIterator<'a> {
    file: &'a mut File,
    position: u64,
    end_position: u64,
    buffer: Vec<u8>,
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Result<(u64, WalEntry)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.end_position {
            return None;
        }
        
        // Remember the current position
        let entry_pos = self.position;
        
        // Read more data if needed
        let result = self.file.read(&mut self.buffer);
        
        match result {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    return None;
                }
                
                self.buffer.truncate(bytes_read);
                
                // Parse the entry
                match WalEntry::from_bytes(&self.buffer) {
                    Ok((entry, bytes_consumed)) => {
                        // Update position
                        self.position += bytes_consumed as u64;
                        
                        // Seek to the next entry
                        if let Err(e) = self.file.seek(SeekFrom::Start(self.position)) {
                            return Some(Err(WalError::Io(e)));
                        }
                        
                        Some(Ok((entry_pos, entry)))
                    }
                    Err(e) => Some(Err(e.into())),
                }
            }
            Err(e) => Some(Err(WalError::Io(e))),
        }
    }
}
