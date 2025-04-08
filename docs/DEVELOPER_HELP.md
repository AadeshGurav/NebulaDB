# 🧑‍💻 Developer Help Guide

This doc helps contributors dig into NebulaDB internals.

---

## 📁 Crate Layout

| Crate         | Description                        |
|---------------|------------------------------------|
| `core`        | Shared types, config, serialization|
| `storage`     | Low-level I/O, compression         |
| `index`       | B-Tree & indexing                  |
| `wal`         | Write-Ahead Logging                |
| `query`       | Query parser & engine (WIP)        |
| `cli`         | Command line interface             |
| `archive`     | Export/import utilities            |
| `server`      | HTTP/gRPC server (planned)         |

---

## ⚙️ Dev Tips

- Run with `RUST_LOG=debug`
- Use `cargo test -- --nocapture`
- `target/debug/nebuladb` is the CLI binary
- WAL and temp storage paths go in `/tmp/nebuladb/`
