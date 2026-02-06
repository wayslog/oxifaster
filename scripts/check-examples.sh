#!/usr/bin/env bash
set -e

# Switch to repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Building examples..."
cargo build --examples --verbose

echo "==> Running example: checkpoint_recovery"
cargo run --example checkpoint_recovery --verbose

echo "==> Running example: incremental_checkpoint"
cargo run --example incremental_checkpoint --verbose

echo "==> Running example: ops_self_check"
cargo run --example ops_self_check --verbose

echo "==> Running Prometheus example (feature=prometheus): prometheus_metrics"
cargo run --example prometheus_metrics --features prometheus --verbose

echo "==> Examples check passed!"

