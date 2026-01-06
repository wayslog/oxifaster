#!/usr/bin/env bash
set -e

# 切换到项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Compiling benchmarks..."
cargo bench --no-run
echo "==> Benchmark compilation passed!"
