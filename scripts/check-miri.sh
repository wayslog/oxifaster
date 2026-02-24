#!/usr/bin/env bash
set -euo pipefail

# Switch to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Running Miri tests..."
# Run only the miri_basic integration test under Miri.
# Most library tests are skipped because Miri does not support
# file I/O, network, io_uring, or heavy thread synchronisation.
cargo +nightly miri test --test miri_basic 2>&1
echo "==> Miri tests passed!"
