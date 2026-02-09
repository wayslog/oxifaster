# Fuzz-Style Stress Tests (Public API)

This repository includes a set of large-scale randomized stress tests that exercise oxifaster's
**public API scenarios** (KV store, checkpoints/recovery, operational helpers, log, codecs, and
async sessions) with a focus on stability (panics, error handling, and memory-related regressions).

These tests are implemented as `#[ignore]` integration tests under `tests/` to keep the default
`cargo test` loop fast.

## Run

```bash
./scripts/check-fuzz.sh
```

### Tunables

- `OXIFASTER_FUZZ_SEED` (u64): base seed used to derive per-test seeds.
- `OXIFASTER_FUZZ_STEPS` (usize): number of operations/iterations per test.
- `OXIFASTER_FUZZ_KEY_SPACE` (u64): key-space size for KV tests.
- `OXIFASTER_FUZZ_STRICT` (bool): when enabled, some targets turn "anomalies" into hard failures.
- `OXIFASTER_FUZZ_VERBOSE` (bool): print a short summary line per target (use with `--nocapture`).
- `OXIFASTER_FUZZ_PENDING_TIMEOUT_MS` (u64): per-attempt timeout (milliseconds) used by the pending-I/O fuzz target when calling `Session::complete_pending_with_timeout`.
  Defaults to 5ms to keep the stress suite bounded; increase when you want to validate end-to-end completion latency.

## Diagnostics (pending I/O target)

When running with `OXIFASTER_FUZZ_VERBOSE=1`, the pending I/O stress target prints a compact
`complete_pending={...}` block. Key fields:

- `calls`: number of `complete_pending_with_timeout` calls performed by the target.
- `timeouts`: number of calls that returned `false` (timed out or did not drain).
- `max_pending` / `max_retry`: max `ThreadContext::{pending_count,retry_count}` observed after calling `complete_pending_with_timeout`.
- `max_timeout_pending` / `max_timeout_retry`: max context counters observed on timeout.
- `timeouts_with_pending` / `timeouts_with_retry`: how often the timeout coincided with a non-zero counter.

Heuristic interpretation:
- If `pending_io_completed` is high while `max_pending` stays near 0 but `max_retry` grows, the bottleneck is likely
  in draining retry requests / pending-read bookkeeping rather than in the I/O worker itself.

Example:

```bash
OXIFASTER_FUZZ_SEED=1 OXIFASTER_FUZZ_STEPS=100000 OXIFASTER_FUZZ_KEY_SPACE=20000 ./scripts/check-fuzz.sh
```

## Notes

- For deeper memory diagnostics, consider running the stress suite under sanitizers/profilers
  appropriate for your toolchain and platform.
