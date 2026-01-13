#!/usr/bin/env bash
set -e

# Switch to repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Running tests (all features)..."
#
# Note: CI requires this job to be "tests only" (no doctests) to avoid
# feature-gated documentation examples causing false negatives.
cargo test --all-features --lib --tests --verbose
echo "==> All tests (all features) passed!"
