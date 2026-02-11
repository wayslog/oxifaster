#!/usr/bin/env bash
set -euo pipefail

# Run selected YCSB Criterion cases for a fixed number of rounds and report the
# median of per-run center estimates from `time: [low center high]`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RUNS=5
BENCH_NAME="ycsb"
FEATURES=""

usage() {
  cat <<USAGE
Usage: $0 [options] [workload ...]

Options:
  -r, --runs N          Number of benchmark rounds per workload (default: $RUNS)
  -f, --features LIST   Cargo features passed to 'cargo bench'
  -b, --bench NAME      Bench target name (default: $BENCH_NAME)
  -h, --help            Show this help message

Examples:
  $0 --runs 7 "ycsb_txn/batch/B_read95_update5/hotspot_80_20"
  $0 -r 5 -f index-profile      "ycsb_txn/batch/B_read95_update5/hotspot_80_20"      "ycsb_txn/batch/C_read100/uniform"
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--runs)
      RUNS="$2"
      shift 2
      ;;
    -f|--features)
      FEATURES="$2"
      shift 2
      ;;
    -b|--bench)
      BENCH_NAME="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--runs must be a positive integer, got: $RUNS" >&2
  exit 2
fi

if [[ $# -eq 0 ]]; then
  set --     "ycsb_txn/batch/B_read95_update5/hotspot_80_20"     "ycsb_txn/batch/C_read100/uniform"
fi

run_workload() {
  local workload="$1"
  local -a run_ns=()

  echo "==> Workload: $workload"

  for ((i = 1; i <= RUNS; i++)); do
    local -a cmd=(cargo bench --bench "$BENCH_NAME")
    if [[ -n "$FEATURES" ]]; then
      cmd+=(--features "$FEATURES")
    fi
    cmd+=(-- --noplot "$workload")

    local output
    if ! output="$(cd "$REPO_ROOT" && "${cmd[@]}" 2>&1)"; then
      echo "$output"
      echo "Benchmark command failed on run $i/$RUNS" >&2
      return 1
    fi

    local parsed
    parsed="$(BENCH_OUTPUT="$output" python3 - <<'PY2'
import os
import re
import sys

text = os.environ.get("BENCH_OUTPUT", "")
cleaned = re.sub(r"\x1b\[[0-9;]*m", "", text)
pat = re.compile(
    r"time:\s+\[\s*[0-9.]+\s*(ns|us|µs|μs|ms|s)\s+([0-9.]+)\s*(ns|us|µs|μs|ms|s)"
)
matches = list(pat.finditer(cleaned))
if not matches:
    sys.exit(1)

m = matches[-1]
value = float(m.group(2))
unit = m.group(3)
if unit == "μs":
    unit = "µs"

factor = {
    "ns": 1.0,
    "us": 1_000.0,
    "µs": 1_000.0,
    "ms": 1_000_000.0,
    "s": 1_000_000_000.0,
}[unit]

print(f"{value * factor}\t{value:.3f} {unit}")
PY2
)" || {
      echo "$output"
      echo "Failed to parse Criterion output on run $i/$RUNS" >&2
      return 1
    }

    local ns="${parsed%%$'	'*}"
    local center="${parsed#*$'	'}"
    run_ns+=("$ns")
    echo "  run $i/$RUNS: $center"
  done

  python3 - "$workload" "${run_ns[@]}" <<'PY2'
import statistics
import sys

workload = sys.argv[1]
vals = [float(v) for v in sys.argv[2:]]
med = statistics.median(vals)


def fmt_ns(ns: float) -> str:
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.3f} s"
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f} µs"
    return f"{ns:.3f} ns"

print(f"  median(center) over {len(vals)} runs: {fmt_ns(med)}")
print(f"  min/max(center): {fmt_ns(min(vals))} / {fmt_ns(max(vals))}")
print(f"  workload_done: {workload}")
PY2
}

for workload in "$@"; do
  run_workload "$workload"
  echo
done
