# 🌌 NebulaDB

![Rust](https://img.shields.io/badge/Rust-🦀-orange)
![License: BSL](https://img.shields.io/badge/license-BSL%201.1-blue)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen)

**NebulaDB** is a high-performance, developer-first document database engine, built from scratch in Rust.  
It combines raw control over data layout with the simplicity of JSON-like querying — giving developers the power to explore, scale, and fully own their data stack.

Designed for modern production systems, **NebulaDB** is distributed by design and supports multiple concurrent connections across various interfaces, making it ideal for real-world, scalable applications.

> 🔐 Free for indie devs. 🔥 Tuned for speed. ⚙️ Built for extensibility.


## ✨ Why NebulaDB?

- 🚀 **Custom storage engine** — raw binary blocks, direct-to-disk layout, no third-party dependencies  
- 🔄 **WAL + crash recovery** — safety-first, journaling-based durability  
- 🔎 **B-Tree indexing** — blazing fast document lookups by ID  
- 📦 **Pluggable architecture** — core, storage, query, CLI, graph layer all modular  
- 🧠 **NLP-powered querying** *(coming soon)* — write your queries in English  
- ⚖️ **BSL-licensed** — Free for solo devs, commercial license for production use  


## 🧬 Internal Architecture

NebulaDB is designed with modularity and performance at its core.

```text
core/        → Shared types, serialization, config
storage/     → Block engine, I/O layout, compression
index/       → ID + (soon) secondary indexing
wal/         → Write-ahead log system
query/       → Execution engine + parser (planned)
cli/         → REPL + JSON query interface
apps/server/ → Optional HTTP server / gRPC API
```


## Components

- **Interface Manager:** Coordinates access to databases through CLI, HTTP, and gRPC
- **Connection Pool:** Manages and reuses database connections for optimal performance
- **Database Manager:** Thread-safe access and operations on multiple databases
- **Write-Ahead Log (WAL):** Ensures data durability and ACID transaction support
- **Storage Engine:** Fast, compressed document storage + future secondary indexes


## 🚀 Quick Start
```sh
git clone https://github.com/AadeshGurav/NebulaDB.git
cd NebulaDB
cargo build
cargo run
```

## 🧩 Features

- **Multi-Interface Support:** CLI, HTTP REST API, and gRPC interfaces
- **High Concurrency:** Thousands of concurrent connections
- **Thread Safety:** Advanced sync primitives for safe access
- **Connection Pooling:** Efficient connection reuse
- **Transaction Support:** Full ACID compliance, with configurable timeouts
- **Configurable:** JSON-driven, production-ready configuration system


## 🛠 Production-Ready Design

### Thread Safety
- `RwLock` for read-heavy access
- `Mutex` for exclusive access
- Minimal contention via efficient locking strategies

### Connection Management
- Pooling with timeout & max limit
- Prevents leaks and resource exhaustion

### High Concurrency
- Thread pools and non-blocking I/O
- Configurable threading via JSON


## ⚙️ Configuration

NebulaDB is configured via a simple JSON file:

```json
{
  "data_dir": "./data",
  "concurrency": {
    "max_databases": 10,
    "max_collections_per_db": 100,
    "background_threads": 4,
    "use_transactions": true,
    "transaction_timeout": 30
  },
  "interfaces": {
    "enable_cli": true,
    "http": {
      "enabled": true,
      "port": 8080,
      "pool": {
        "max_connections": 1000,
        "timeout": 30,
        "keep_alive": 60,
        "max_requests_per_connection": 10000
      }
    },
    "grpc": {
      "enabled": true,
      "port": 50051,
      "pool": {
        "max_connections": 1000,
        "timeout": 30,
        "max_stream_duration": 600,
        "max_concurrent_streams": 100
      }
    }
  }
}
```


## 🏗 Starting in Production Mode

To run NebulaDB with all interfaces enabled and optimized:

```sh
./nebuladb --production --config config.json
```


## 📊 Monitoring & Metrics

- Active connections
- Connection pool stats
- Query performance metrics
- Disk usage & WAL status

All accessible via HTTP metrics endpoint.


## 📘 Docs
- [Contribution Guide](docs/CONTIRBUTION.md)
- [Roadmap](docs/ROADMAP.md)
- [Dev Help](docs/DEVELOPER_HELP.md)


## 📜 License
Business Source License 1.1 (BSL)
Free for individuals, education, and open-source R&D.
Commercial licenses available via `Aadesh Gurav`.


## 🛣 Coming Soon

- 🧠 Natural Language Querying (NLP Interface)
- 🔍 Full-Text Search + Secondary Indexes
- 🧰 Built-in Web Dashboard
- 🌐 Cluster Mode & Sharding



Made with 🦀 & ☕ by @AadeshGurav
