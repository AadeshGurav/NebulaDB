# 🌌 NebulaDB

**NebulaDB** is a high-performance, developer-first document database engine, written in Rust from scratch. It blends raw control over data layout with the elegance of JSON-like querying — empowering developers to explore, scale, and own their data stack.

> 🔐 Free for indie devs. 🔥 Tuned for speed. ⚙️ Built for extensibility.

---

## ✨ Why NebulaDB?

- 🚀 **Custom storage engine** — raw binary blocks, direct-to-disk layout, no third-party dependencies
- 🔄 **WAL + crash recovery** — safety-first, journaling-based durability
- 🔎 **B-Tree indexing** — blazing fast document lookups by ID
- 📦 **Pluggable architecture** — core, storage, query, CLI, graph layer all modular
- 🧠 **NLP-powered querying** *(coming soon)* — write your queries in English
- ⚖️ **BSL-licensed** — Free for solo devs, commercial license for production use

---

## 📦 Architecture (Work in Progress)

```text
core/        → Shared types, serialization, config
storage/     → Block engine, I/O layout, compression
index/       → ID + (soon) secondary indexing
wal/         → Write-ahead log system
query/       → Execution engine + parser (planned)
cli/         → REPL + JSON query interface
apps/server/ → Optional HTTP server / gRPC API
```


## 🚀 Quick Start
```sh
git clone https://github.com/AadeshGurav/NebulaDB.git
cd NebulaDB
cargo build
cargo run
```

---

## 📜 License
Business Source License 1.1 (BSL)
Free for individuals, education, and open-source R&D.
Commercial licenses available via `Aadesh Gurav`.


## 📘 Docs
- [Contribution Guide](docs/CONTIRBUTION.md)
- [Roadmap](docs/ROADMAP.md)
- [Dev Help](docs/DEVELOPER_HELP.md)
