#!/bin/bash

# Fix warnings in all crates
cargo fix --allow-dirty --workspace

# Fix specific module warnings
modules=("core" "storage" "wal" "index" "query" "archive" "graph")

for module in "${modules[@]}"; do
  echo "Fixing warnings in nebuladb-$module"
  cargo fix --lib -p "nebuladb-$module" --allow-dirty
done

# Fix the main binary
cargo fix --bin "nebuladb" --allow-dirty

echo "All warnings fixed!"
