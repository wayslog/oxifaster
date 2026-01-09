#!/usr/bin/env bash
set -e

# Switch to repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Running tests (all features)..."
cargo test --all-features --verbose
echo "==> All tests (all features) passed!"

