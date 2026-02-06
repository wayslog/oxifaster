//! Shared helpers for runnable examples.
//!
//! This module lives under `examples/util/` so Cargo does not treat it as a
//! standalone example binary.

use oxifaster::stats::StatsSnapshot;

#[cfg(feature = "prometheus")]
use oxifaster::stats::prometheus::PrometheusRenderer;

/// Assert the store produced at least one externally observable signal.
///
/// The intent is to ensure examples exercise real code paths (not just compile).
pub fn assert_observable_activity(snapshot: &StatsSnapshot) {
    let has_activity = snapshot.total_operations > 0
        || snapshot.checkpoints_started > 0
        || snapshot.compactions_started > 0
        || snapshot.index_grows_started > 0
        || snapshot.recoveries_started > 0
        || snapshot.pending_io_submitted > 0;

    assert!(
        has_activity,
        "expected observable activity; got snapshot={snapshot:?}"
    );
}

/// Render and print Prometheus text exposition for a stats snapshot (when enabled).
#[cfg(feature = "prometheus")]
pub fn print_prometheus(snapshot: &StatsSnapshot) {
    let text = PrometheusRenderer::new().render_snapshot(snapshot);
    println!("\n--- Prometheus metrics (stats snapshot) ---\n{text}");
    assert!(text.contains("oxifaster_operations_total"));
}

/// No-op on builds without the `prometheus` feature.
#[cfg(not(feature = "prometheus"))]
pub fn print_prometheus(_snapshot: &StatsSnapshot) {}
