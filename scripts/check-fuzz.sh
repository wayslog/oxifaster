#!/usr/bin/env bash
set -euo pipefail

# Run libFuzzer-based fuzzing via cargo-fuzz.
#
# Modes:
# - local (default): 30 minutes
# - ci: 5 minutes
#
# Tunables (optional):
# - OXIFASTER_FUZZ_MAX_TOTAL_TIME (seconds): override mode duration
# - OXIFASTER_FUZZ_TIMEOUT (seconds): per-input timeout
# - OXIFASTER_FUZZ_MAX_LEN (bytes): max input size
# - OXIFASTER_FUZZ_RSS_LIMIT_MB (MB): memory limit
#
# Examples:
#   ./scripts/check-fuzz.sh
#   ./scripts/check-fuzz.sh ci
#   OXIFASTER_FUZZ_MAX_TOTAL_TIME=120 ./scripts/check-fuzz.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

MODE="${1:-local}"

case "$MODE" in
local)
  DEFAULT_MAX_TOTAL_TIME=1800
  ;;
ci)
  DEFAULT_MAX_TOTAL_TIME=300
  ;;
*)
  echo "Usage: $0 [local|ci]"
  exit 2
  ;;
esac

MAX_TOTAL_TIME="${OXIFASTER_FUZZ_MAX_TOTAL_TIME:-$DEFAULT_MAX_TOTAL_TIME}"
TIMEOUT_PER_INPUT="${OXIFASTER_FUZZ_TIMEOUT:-20}"
MAX_LEN="${OXIFASTER_FUZZ_MAX_LEN:-4096}"
RSS_LIMIT_MB="${OXIFASTER_FUZZ_RSS_LIMIT_MB:-4096}"

declare -a FUZZ_CMD=()

# Prefer explicit nightly invocations first.
if command -v rustup >/dev/null 2>&1 && rustup run nightly cargo fuzz --help >/dev/null 2>&1; then
  FUZZ_CMD=(rustup run nightly cargo fuzz)
elif cargo +nightly fuzz --help >/dev/null 2>&1; then
  FUZZ_CMD=(cargo +nightly fuzz)
elif cargo fuzz --help >/dev/null 2>&1 && rustc --version | grep -q "nightly"; then
  # Fallback for environments where current cargo/rustc are already nightly.
  FUZZ_CMD=(cargo fuzz)
else
  echo "cargo-fuzz with nightly is required."
  echo "Install with:"
  echo "  rustup toolchain install nightly"
  echo "  cargo install cargo-fuzz"
  echo
  echo "Diagnostics:"
  echo "  cargo version: $(cargo --version 2>/dev/null || echo unavailable)"
  if command -v rustup >/dev/null 2>&1; then
    echo "  rustup version: $(rustup --version | head -n1)"
    echo "  installed toolchains:"
    rustup toolchain list || true
  else
    echo "  rustup: not found"
  fi
  exit 1
fi

echo "==> Running cargo-fuzz target: fasterkv_public_api"
echo "    command=${FUZZ_CMD[*]}"
echo "    mode=$MODE max_total_time=${MAX_TOTAL_TIME}s timeout=${TIMEOUT_PER_INPUT}s max_len=${MAX_LEN} rss_limit_mb=${RSS_LIMIT_MB}"

"${FUZZ_CMD[@]}" run fasterkv_public_api -- \
  -max_total_time="${MAX_TOTAL_TIME}" \
  -timeout="${TIMEOUT_PER_INPUT}" \
  -max_len="${MAX_LEN}" \
  -rss_limit_mb="${RSS_LIMIT_MB}"

echo "==> cargo-fuzz completed successfully!"
