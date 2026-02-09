# Public API Examples Matrix

This document maps oxifaster's public API surface to runnable examples and externally observable metrics.

Conventions:
- Each **feature** is represented by one **primary example** under `examples/`.
- Each example should (a) exercise the relevant API paths, (b) assert the behavior, and (c) emit
  Prometheus text exposition (when `--features prometheus`) with the `oxifaster_` prefix.
- Metrics are described as names as exposed by `oxifaster::stats::prometheus::PrometheusRenderer`.

## Key-Value Store (FasterKv)

| Feature | Primary public API | Example | Metrics (minimum) |
|---|---|---|---|
| CRUD + sessions | `FasterKv::new`, `start_session`, `Session::{read,upsert,delete}` | `basic_kv` | `oxifaster_operations_total`, `oxifaster_reads_total`, `oxifaster_upserts_total`, `oxifaster_deletes_total` |
| RMW | `Session::rmw` | `async_operations` (sync + async patterns) | `oxifaster_rmws_total` |
| Concurrency | multi-session usage across threads | `concurrent_access` | `oxifaster_operations_total`, `oxifaster_throughput_ops_per_second` |
| Async I/O / pending | `AsyncSession`, pending I/O completion path | `async_operations` | `oxifaster_pending_io_submitted_total`, `oxifaster_pending_io_completed_total` |
| Read cache | `FasterKv::with_read_cache` (or store config + cache) | `read_cache` | `oxifaster_read_hits_total`, `oxifaster_hit_rate_ratio` |
| Compaction | `should_compact`, `log_compact` | `compaction` | `oxifaster_compactions_started_total`, `oxifaster_compactions_completed_total` |
| Index growth | `grow_index` / growth trigger APIs | `index_growth` | `oxifaster_index_grows_started_total`, `oxifaster_index_grows_completed_total` |
| Checkpoint (full) | `checkpoint`, `checkpoint_with_callbacks` | `checkpoint_recovery` | `oxifaster_checkpoints_started_total`, `oxifaster_checkpoints_completed_total` |
| Recovery | `FasterKv::recover*`, `get_recovered_sessions`, `continue_session` | `checkpoint_recovery` | `oxifaster_recoveries_started_total` |
| Incremental checkpoint (delta log) | `checkpoint_full_snapshot`, `checkpoint_incremental`, `CheckpointInfo` | `incremental_checkpoint` | `oxifaster_checkpoints_started_total`, `oxifaster_checkpoints_completed_total` |

Note: incremental checkpoint recovery from an incremental token is not currently demonstrated via `FasterKv::recover` in examples.
The example validates the persisted delta artifacts and metadata linkage to the base snapshot.

## Standalone Log (FasterLog)

| Feature | Primary public API | Example | Metrics (minimum) |
|---|---|---|---|
| Append + commit + scan | `FasterLog` APIs under `oxifaster::log` | `faster_log`, `log_scan` | `oxifaster_faster_log_tail_address`, `oxifaster_faster_log_committed_address`, `oxifaster_faster_log_begin_address` |

## Configuration / Types / Utilities

| Feature | Primary public API | Example | Metrics (minimum) |
|---|---|---|---|
| Config loading | `OxifasterConfig::load_from_env` | `config_store` | (N/A) |
| Custom codecs/types | `codec::*`, `PersistKey/PersistValue` | `custom_types` | (N/A) |
| Variable-length utilities | `varlen::*`, `codec::Utf8` | `variable_length` | (N/A) |
| Device backends | `device::{NullDisk,FileSystemDisk,IoUringDevice}` | `io_uring` | `oxifaster_pending_io_submitted_total` (when I/O path is used) |

## Operational Helpers (ops)

| Feature | Primary public API | Example | Metrics (minimum) |
|---|---|---|---|
| Checkpoint self-check + recover-latest | `ops::{checkpoint_self_check,recover_latest*}` | `ops_self_check` | `oxifaster_checkpoint_self_check_total`, `oxifaster_checkpoint_self_check_invalid` |

## Prometheus Export

| Feature | Primary public API | Example | Metrics (minimum) |
|---|---|---|---|
| Prometheus text exposition | `stats::prometheus::{PrometheusRenderer,...}` | `prometheus_metrics` | `oxifaster_operations_total`, `oxifaster_uptime_seconds` |
| Minimal HTTP `/metrics` server | `stats::prometheus::MetricsHttpServer` | `prometheus_metrics` | `oxifaster_operations_total` |
