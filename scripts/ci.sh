#!/usr/bin/env bash
set -e

# 切换到项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "========================================"
echo "        Running Full CI Checks          "
echo "========================================"

# 1. Format check
"$SCRIPT_DIR/check-fmt.sh"

# 2. Clippy
"$SCRIPT_DIR/check-clippy.sh"

# 3. Build
"$SCRIPT_DIR/check-build.sh"

# 4. Tests
"$SCRIPT_DIR/check-test.sh"

# 5. Bench compilation
"$SCRIPT_DIR/check-bench.sh"

echo "========================================"
echo "        All CI Checks Passed!           "
echo "========================================"
