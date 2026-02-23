#!/usr/bin/env bash
set -euo pipefail

# Switch to repository root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

MODE="${1:-core}"
TEST_FILTER="${OXIFASTER_TEST_FILTER:-}"
SKIP_LIB="${OXIFASTER_TEST_SKIP_LIB:-0}"
declare -a TEST_FILTERS=()
if [[ -n "$TEST_FILTER" ]]; then
  normalized_filters="${TEST_FILTER//|/,}"
  IFS=',' read -ra raw_filters <<< "$normalized_filters"
  for filter in "${raw_filters[@]}"; do
    # Trim leading/trailing whitespace.
    filter="${filter#"${filter%%[![:space:]]*}"}"
    filter="${filter%"${filter##*[![:space:]]}"}"
    if [[ -n "$filter" ]]; then
      TEST_FILTERS+=("$filter")
    fi
  done
fi

run_test_target() {
  local target_kind="$1"
  local target_name="$2"
  if [[ ${#TEST_FILTERS[@]} -eq 0 ]]; then
    cargo test "$target_kind" "$target_name"
    return
  fi

  local match_count=0
  local filter=""
  for filter in "${TEST_FILTERS[@]}"; do
    match_count=$(
      cargo test "$target_kind" "$target_name" "$filter" -- --list 2>/dev/null \
        | awk '/: test$/{c++} END{print c+0}'
    )
    if [[ "$match_count" -eq 0 ]]; then
      echo "error: test filter '$filter' matched 0 tests for target '$target_name'" >&2
      exit 1
    fi
    cargo test "$target_kind" "$target_name" "$filter"
  done
}

contains_target() {
  local needle="$1"
  shift
  local candidate=""
  for candidate in "$@"; do
    if [[ "$candidate" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

run_lib_tests() {
  if [[ ${#TEST_FILTERS[@]} -eq 0 ]]; then
    cargo test --lib
    return
  fi

  local match_count=0
  local filter=""
  for filter in "${TEST_FILTERS[@]}"; do
    match_count=$(
      cargo test --lib "$filter" -- --list 2>/dev/null \
        | awk '/: test$/{c++} END{print c+0}'
    )
    if [[ "$match_count" -eq 0 ]]; then
      echo "error: test filter '$filter' matched 0 lib tests" >&2
      exit 1
    fi
    cargo test --lib "$filter"
  done
}

run_core_tests() {
  echo "==> Running core tests (lib + selected integration targets)..."
  if [[ "$SKIP_LIB" == "1" ]]; then
    echo "==> Core filter: skipping lib tests (OXIFASTER_TEST_SKIP_LIB=1)"
  else
    if [[ ${#TEST_FILTERS[@]} -gt 0 ]]; then
      echo "==> Core filter: test_filter=${TEST_FILTERS[*]}"
    fi
    run_lib_tests
  fi

  # Keep the default lane fast, but allow local/CI overrides.
  #
  # Tunables:
  # - OXIFASTER_TEST_CORE_TARGETS="target_a,target_b"
  #     Override the default core integration target allowlist.
  # - OXIFASTER_TEST_RUN_EXTENDED=1
  #     Run all integration targets in core mode (disables default allowlist).
  # - OXIFASTER_TEST_SKIP_PREFIXES="<regex>"
  #     Override default skip prefixes when extended mode is enabled.
  # - OXIFASTER_TEST_SKIP_REGEX="<regex>"
  #     Additional skip regex.
  # - OXIFASTER_TEST_TARGETS="target_a,target_b"
  #     Run only selected integration test targets.
  local default_core_targets=(
    async_session
    auto_compaction
    basic_ops
    checkpoint
    checkpoint_concurrent
    checkpoint_crash
    checkpoint_integrity
    checkpoint_locks
    codec_bincode
    cold_index
    compaction
    compatibility
    conditional_insert
    cpr_checkpoint_under_load
  )

  local default_skip_prefixes='^(coverage_|fuzz_|loom_|miri_)'
  local core_targets_raw="${OXIFASTER_TEST_CORE_TARGETS:-}"
  local run_extended="${OXIFASTER_TEST_RUN_EXTENDED:-0}"
  local skip_prefixes="${OXIFASTER_TEST_SKIP_PREFIXES:-$default_skip_prefixes}"
  local skip_regex="${OXIFASTER_TEST_SKIP_REGEX:-}"
  local selected_targets_raw="${OXIFASTER_TEST_TARGETS:-}"
  if [[ "$run_extended" == "1" ]]; then
    skip_prefixes=""
  fi

  local selected_targets=()
  if [[ -n "$selected_targets_raw" ]]; then
    selected_targets_raw="${selected_targets_raw//,/ }"
    for target in $selected_targets_raw; do
      selected_targets+=("$target")
    done
  fi

  local core_targets=("${default_core_targets[@]}")
  if [[ -n "$core_targets_raw" ]]; then
    core_targets=()
    core_targets_raw="${core_targets_raw//,/ }"
    for target in $core_targets_raw; do
      core_targets+=("$target")
    done
  fi

  if [[ "$run_extended" != "1" ]] && [[ ${#selected_targets[@]} -eq 0 ]]; then
    echo "==> Core profile: allowlist (${#core_targets[@]} targets)"
  fi

  if [[ -n "$skip_prefixes" ]]; then
    echo "==> Core filter: skip_prefixes=$skip_prefixes"
  fi
  if [[ -n "$skip_regex" ]]; then
    echo "==> Core filter: skip_regex=$skip_regex"
  fi
  if [[ ${#selected_targets[@]} -gt 0 ]]; then
    echo "==> Core filter: selected_targets=${selected_targets[*]}"
  fi

  local integration_tests=()
  while IFS= read -r test_target; do
    integration_tests+=("$test_target")
  done < <(find tests -maxdepth 1 -type f -name "*.rs" -exec basename {} .rs \; | sort)

  if [[ "$run_extended" != "1" ]] && [[ ${#selected_targets[@]} -eq 0 ]]; then
    for target in "${core_targets[@]}"; do
      if ! contains_target "$target" "${integration_tests[@]}"; then
        echo "error: core target '$target' does not exist under tests/" >&2
        exit 1
      fi
    done
  fi

  for test_target in "${integration_tests[@]}"; do
    if [[ ${#selected_targets[@]} -gt 0 ]]; then
      local is_selected=0
      local selected_target=""
      for selected_target in "${selected_targets[@]}"; do
        if [[ "$test_target" == "$selected_target" ]]; then
          is_selected=1
          break
        fi
      done
      if [[ "$is_selected" -eq 0 ]]; then
        echo "==> Skipping non-selected test target: $test_target"
        continue
      fi
    elif [[ "$run_extended" != "1" ]]; then
      if ! contains_target "$test_target" "${core_targets[@]}"; then
        echo "==> Skipping non-core target: $test_target"
        continue
      fi
    fi

    if [[ -n "$skip_prefixes" ]] && [[ "$test_target" =~ $skip_prefixes ]]; then
      echo "==> Skipping prefixed test target: $test_target"
      continue
    fi

    if [[ -n "$skip_regex" ]] && [[ "$test_target" =~ $skip_regex ]]; then
      echo "==> Skipping filtered test target: $test_target"
      continue
    fi

    echo "==> Running integration test target: $test_target"
    run_test_target --test "$test_target"
  done
}

run_thin_tests() {
  echo "==> Running thin tests (targeted lane)..."
  if [[ ${#TEST_FILTERS[@]} -gt 0 ]]; then
    echo "==> Thin filter: test_filter=${TEST_FILTERS[*]}"
  fi
  if [[ "$SKIP_LIB" == "1" ]]; then
    echo "==> Thin filter: skipping lib tests (OXIFASTER_TEST_SKIP_LIB=1)"
  else
    run_lib_tests
  fi

  local selected_targets_raw="${OXIFASTER_TEST_TARGETS:-}"
  local selected_targets=()
  if [[ -n "$selected_targets_raw" ]]; then
    selected_targets_raw="${selected_targets_raw//,/ }"
    for target in $selected_targets_raw; do
      selected_targets+=("$target")
    done
  fi

  if [[ ${#selected_targets[@]} -gt 0 ]]; then
    for test_target in "${selected_targets[@]}"; do
      echo "==> Running thin integration target: $test_target"
      run_test_target --test "$test_target"
    done
  fi
}

case "$MODE" in
core)
  run_core_tests
  ;;
full)
  echo "==> Running tests (full: includes doctests)..."
  if [[ -n "$TEST_FILTER" ]]; then
    cargo test "$TEST_FILTER"
  else
    cargo test
  fi
  ;;
thin)
  run_thin_tests
  ;;
*)
  echo "Usage: $0 [core|thin|full]"
  echo "  core (default): lib + filtered integration targets"
  echo "  thin: lib + explicitly selected integration targets only"
  echo "  full: all tests including doctests"
  echo "  env OXIFASTER_TEST_FILTER=<p1|p2|...>: pass one or more test filters to cargo test"
  echo "  env OXIFASTER_TEST_CORE_TARGETS=<t1,t2,...>: override default core integration allowlist"
  echo "  env OXIFASTER_TEST_SKIP_LIB=1: skip lib tests in core/thin mode"
  echo "  env OXIFASTER_TEST_RUN_EXTENDED=1: run all integration targets in core mode"
  exit 2
  ;;
esac

echo "==> All tests passed!"
