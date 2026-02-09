#!/usr/bin/env bash
set -euo pipefail

# Run large-scale randomized "fuzz-style" stress tests for public API scenarios.
#
# By default these tests are marked as `#[ignore]` to keep the regular CI/test
# cycle fast. This script runs ignored tests only.
#
# Tunables (optional):
# - OXIFASTER_FUZZ_SEED: base seed (u64)
# - OXIFASTER_FUZZ_STEPS: number of operations/iterations per test (usize)
# - OXIFASTER_FUZZ_KEY_SPACE: key space size (u64)
#
# Example:
#   OXIFASTER_FUZZ_SEED=1 OXIFASTER_FUZZ_STEPS=100000 ./scripts/check-fuzz.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Running ignored fuzz-style tests..."

cargo test --test fuzz_kv -- --ignored --nocapture
cargo test --test fuzz_kv_varlen -- --ignored --nocapture
cargo test --test fuzz_kv_pending -- --ignored --nocapture
cargo test --test fuzz_concurrent -- --ignored --nocapture
cargo test --test fuzz_ops -- --ignored --nocapture
cargo test --test fuzz_log -- --ignored --nocapture
cargo test --test fuzz_types -- --ignored --nocapture
cargo test --test fuzz_async -- --ignored --nocapture

echo "==> Fuzz-style tests passed!"
