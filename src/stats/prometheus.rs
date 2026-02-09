//! Prometheus text exposition for oxifaster statistics.
//!
//! This module intentionally avoids adding a Prometheus client dependency.
//! It provides a small, stable mapping from `StatsSnapshot` (and a few
//! auxiliary public types) to the Prometheus text exposition format so
//! external systems can scrape metrics.
//!
//! Metric naming conventions:
//! - All metrics are prefixed with `oxifaster_`.
//! - Counters use the `_total` suffix.
//! - Durations are reported in seconds (`*_seconds`).
//! - Ratios use the `_ratio` suffix.

use std::fmt::Write as _;

use crate::log::LogStats;
use crate::ops::CheckpointSelfCheckReport;
use crate::stats::{StatsCollector, StatsSnapshot};

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Render options for Prometheus text exposition.
#[derive(Debug, Clone)]
pub struct PrometheusOptions {
    /// Whether to include `# HELP` and `# TYPE` lines.
    pub include_metadata: bool,
}

impl Default for PrometheusOptions {
    fn default() -> Self {
        Self {
            include_metadata: true,
        }
    }
}

/// A renderer for Prometheus text exposition.
#[derive(Debug, Clone)]
pub struct PrometheusRenderer {
    options: PrometheusOptions,
}

impl PrometheusRenderer {
    /// Create a renderer with default options.
    pub fn new() -> Self {
        Self {
            options: PrometheusOptions::default(),
        }
    }

    /// Create a renderer with custom options.
    pub fn with_options(options: PrometheusOptions) -> Self {
        Self { options }
    }

    /// Render a `StatsCollector` by taking a snapshot.
    pub fn render_collector(&self, collector: &StatsCollector) -> String {
        self.render_snapshot(&collector.snapshot())
    }

    /// Render a `StatsSnapshot` as Prometheus text exposition.
    pub fn render_snapshot(&self, snapshot: &StatsSnapshot) -> String {
        let mut out = String::with_capacity(2048);

        // Note: Prometheus text format expects '\n' line endings.
        self.metric_gauge_f64(
            &mut out,
            "oxifaster_uptime_seconds",
            "Seconds since statistics collection started.",
            snapshot.elapsed.as_secs_f64(),
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_operations_total",
            "Total operations (reads + upserts + rmws + deletes + conditional inserts).",
            snapshot.total_operations,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_reads_total",
            "Total read operations.",
            snapshot.reads,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_read_hits_total",
            "Total read hits.",
            snapshot.read_hits,
        );
        let read_misses = snapshot.reads.saturating_sub(snapshot.read_hits);
        self.metric_counter_u64(
            &mut out,
            "oxifaster_read_misses_total",
            "Total read misses.",
            read_misses,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_upserts_total",
            "Total upsert operations.",
            snapshot.upserts,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_rmws_total",
            "Total read-modify-write operations.",
            snapshot.rmws,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_deletes_total",
            "Total delete operations.",
            snapshot.deletes,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_conditional_inserts_total",
            "Total conditional insert operations.",
            snapshot.conditional_inserts,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_conditional_insert_exists_total",
            "Total conditional inserts that found the key already exists.",
            snapshot.conditional_insert_exists,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_pending_total",
            "Total pending operation events recorded by the store.",
            snapshot.pending,
        );

        self.metric_gauge_f64(
            &mut out,
            "oxifaster_hit_rate_ratio",
            "Read hit rate (read_hits / reads).",
            snapshot.hit_rate,
        );

        self.metric_gauge_f64(
            &mut out,
            "oxifaster_throughput_ops_per_second",
            "Approximate overall throughput since start (ops/sec).",
            snapshot.throughput,
        );

        self.metric_gauge_f64(
            &mut out,
            "oxifaster_avg_latency_seconds",
            "Average operation latency (seconds).",
            snapshot.avg_latency.as_secs_f64(),
        );

        // Index
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_index_entries",
            "Number of index entries (approx).",
            snapshot.index_entries,
        );
        self.metric_gauge_f64(
            &mut out,
            "oxifaster_index_load_factor_ratio",
            "Index load factor.",
            snapshot.index_load_factor,
        );

        // Hybrid log
        self.metric_counter_u64(
            &mut out,
            "oxifaster_hlog_bytes_allocated_total",
            "Total bytes allocated by the hybrid log allocator.",
            snapshot.bytes_allocated,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_hlog_pages_flushed_total",
            "Total pages flushed to disk by the hybrid log.",
            snapshot.pages_flushed,
        );

        // Allocator
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_allocator_memory_in_use_bytes",
            "Current memory in use (bytes).",
            snapshot.memory_in_use,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_allocator_peak_memory_bytes",
            "Peak memory usage (bytes).",
            snapshot.peak_memory,
        );

        // Operational lifecycle counters
        self.metric_counter_u64(
            &mut out,
            "oxifaster_checkpoints_started_total",
            "Total checkpoints started.",
            snapshot.checkpoints_started,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_checkpoints_completed_total",
            "Total checkpoints completed successfully.",
            snapshot.checkpoints_completed,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_checkpoints_failed_total",
            "Total checkpoints failed.",
            snapshot.checkpoints_failed,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_compactions_started_total",
            "Total compactions started.",
            snapshot.compactions_started,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_compactions_completed_total",
            "Total compactions completed successfully.",
            snapshot.compactions_completed,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_compactions_failed_total",
            "Total compactions failed.",
            snapshot.compactions_failed,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_index_grows_started_total",
            "Total index growth operations started.",
            snapshot.index_grows_started,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_index_grows_completed_total",
            "Total index growth operations completed successfully.",
            snapshot.index_grows_completed,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_index_grows_failed_total",
            "Total index growth operations failed.",
            snapshot.index_grows_failed,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_recoveries_started_total",
            "Total recovery attempts started.",
            snapshot.recoveries_started,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_recoveries_failed_total",
            "Total recovery attempts failed.",
            snapshot.recoveries_failed,
        );

        self.metric_counter_u64(
            &mut out,
            "oxifaster_pending_io_submitted_total",
            "Total pending I/O submissions recorded.",
            snapshot.pending_io_submitted,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_pending_io_completed_total",
            "Total pending I/O completions recorded.",
            snapshot.pending_io_completed,
        );
        self.metric_counter_u64(
            &mut out,
            "oxifaster_pending_io_failed_total",
            "Total pending I/O failures recorded.",
            snapshot.pending_io_failed,
        );

        out
    }

    /// Render a checkpoint self-check report as Prometheus text exposition.
    pub fn render_checkpoint_self_check_report(
        &self,
        report: &CheckpointSelfCheckReport,
    ) -> String {
        let mut out = String::with_capacity(512);

        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_total",
            "Total checkpoint directories scanned by self-check.",
            report.total as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_valid",
            "Valid checkpoints detected by self-check.",
            report.valid as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_invalid",
            "Invalid checkpoints detected by self-check.",
            report.invalid as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_removed_total",
            "Invalid checkpoints removed during repair.",
            report.removed.len() as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_planned_removals_total",
            "Invalid checkpoints planned for removal (dry-run).",
            report.planned_removals.len() as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_checkpoint_self_check_issues_total",
            "Issues detected during checkpoint self-check validation.",
            report.issues.len() as u64,
        );

        out
    }

    /// Render `FasterLog` stats as Prometheus text exposition.
    pub fn render_log_stats(&self, stats: &LogStats) -> String {
        let mut out = String::with_capacity(256);

        self.metric_gauge_u64(
            &mut out,
            "oxifaster_faster_log_tail_address",
            "FASTER Log tail address (raw control value).",
            stats.tail_address.control(),
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_faster_log_committed_address",
            "FASTER Log committed address (raw control value).",
            stats.committed_address.control(),
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_faster_log_begin_address",
            "FASTER Log begin address (raw control value).",
            stats.begin_address.control(),
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_faster_log_page_size_bytes",
            "FASTER Log page size (bytes).",
            stats.page_size as u64,
        );
        self.metric_gauge_u64(
            &mut out,
            "oxifaster_faster_log_buffer_pages",
            "FASTER Log in-memory buffer pages.",
            stats.buffer_pages as u64,
        );

        out
    }

    fn maybe_metadata(&self, out: &mut String, name: &str, help: &str, ty: &str) {
        if !self.options.include_metadata {
            return;
        }
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {ty}");
    }

    fn metric_counter_u64(&self, out: &mut String, name: &str, help: &str, value: u64) {
        self.maybe_metadata(out, name, help, "counter");
        let _ = writeln!(out, "{name} {value}");
    }

    fn metric_gauge_u64(&self, out: &mut String, name: &str, help: &str, value: u64) {
        self.maybe_metadata(out, name, help, "gauge");
        let _ = writeln!(out, "{name} {value}");
    }

    fn metric_gauge_f64(&self, out: &mut String, name: &str, help: &str, value: f64) {
        self.maybe_metadata(out, name, help, "gauge");
        // Prometheus accepts NaN/Inf, but emitting them is generally unhelpful.
        let v = if value.is_finite() { value } else { 0.0 };
        let _ = writeln!(out, "{name} {v}");
    }
}

impl Default for PrometheusRenderer {
    fn default() -> Self {
        Self::new()
    }
}

/// A minimal HTTP server that serves Prometheus metrics at `GET /metrics`.
///
/// This implementation intentionally avoids external HTTP dependencies.
/// It requires a Tokio runtime to be active.
#[derive(Debug)]
pub struct MetricsHttpServer {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<io::Result<()>>,
}

impl MetricsHttpServer {
    /// Start serving metrics on `bind_addr`.
    ///
    /// If `bind_addr` uses port 0, the OS will assign a free port. Use
    /// [`MetricsHttpServer::local_addr`] to retrieve the final address.
    pub async fn bind(
        bind_addr: SocketAddr,
        render: Arc<dyn Fn() -> String + Send + Sync>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    accept = listener.accept() => {
                        let (mut socket, _peer) = accept?;
                        let render = render.clone();
                        tokio::spawn(async move {
                            let _ = handle_one_connection(&mut socket, &render).await;
                        });
                    }
                }
            }
            Ok(())
        });

        Ok(Self {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            join,
        })
    }

    /// The actual local address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Shut down the server and wait for completion.
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        match self.join.await {
            Ok(r) => r,
            Err(err) => Err(io::Error::other(format!(
                "metrics server join failed: {err}"
            ))),
        }
    }
}

async fn handle_one_connection(
    socket: &mut tokio::net::TcpStream,
    render: &Arc<dyn Fn() -> String + Send + Sync>,
) -> io::Result<()> {
    // Read up to a small buffer. This is sufficient for typical GET requests.
    let mut buf = [0u8; 8192];
    let n = socket.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let req = &buf[..n];

    let (method, path) = parse_request_line(req).unwrap_or(("", ""));
    match (method, path) {
        ("GET", "/metrics") => {
            let body = (render)();
            write_response(
                socket,
                200,
                "OK",
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
            )
            .await
        }
        ("GET", "/healthz") => {
            write_response(socket, 200, "OK", "text/plain; charset=utf-8", "ok\n").await
        }
        _ => {
            write_response(
                socket,
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                "not found\n",
            )
            .await
        }
    }
}

fn parse_request_line(req: &[u8]) -> Option<(&str, &str)> {
    let mut line_end = None;
    for i in 0..req.len().saturating_sub(1) {
        if req[i] == b'\r' && req[i + 1] == b'\n' {
            line_end = Some(i);
            break;
        }
    }
    let line_end = line_end?;
    let line = std::str::from_utf8(&req[..line_end]).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    Some((method, path))
}

async fn write_response(
    socket: &mut tokio::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    let _ = socket.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn render_snapshot_contains_prefix() {
        let snapshot = StatsSnapshot {
            elapsed: Duration::from_secs(1),
            total_operations: 3,
            reads: 1,
            read_hits: 1,
            upserts: 1,
            rmws: 1,
            deletes: 0,
            conditional_inserts: 0,
            conditional_insert_exists: 0,
            pending: 0,
            hit_rate: 1.0,
            throughput: 3.0,
            avg_latency: Duration::from_millis(1),
            index_entries: 10,
            index_load_factor: 0.1,
            bytes_allocated: 0,
            pages_flushed: 0,
            memory_in_use: 0,
            peak_memory: 0,
            checkpoints_started: 0,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            compactions_started: 0,
            compactions_completed: 0,
            compactions_failed: 0,
            index_grows_started: 0,
            index_grows_completed: 0,
            index_grows_failed: 0,
            recoveries_started: 0,
            recoveries_failed: 0,
            pending_io_submitted: 0,
            pending_io_completed: 0,
            pending_io_failed: 0,
        };

        let out = PrometheusRenderer::new().render_snapshot(&snapshot);
        assert!(out.contains("oxifaster_operations_total 3"));
        assert!(out.contains("# TYPE oxifaster_reads_total counter"));
    }

    #[cfg(feature = "prometheus")]
    #[tokio::test]
    async fn http_server_serves_metrics() {
        let snapshot = StatsSnapshot {
            elapsed: Duration::from_secs(1),
            total_operations: 1,
            reads: 1,
            read_hits: 1,
            upserts: 0,
            rmws: 0,
            deletes: 0,
            conditional_inserts: 0,
            conditional_insert_exists: 0,
            pending: 0,
            hit_rate: 1.0,
            throughput: 1.0,
            avg_latency: Duration::from_millis(1),
            index_entries: 0,
            index_load_factor: 0.0,
            bytes_allocated: 0,
            pages_flushed: 0,
            memory_in_use: 0,
            peak_memory: 0,
            checkpoints_started: 0,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            compactions_started: 0,
            compactions_completed: 0,
            compactions_failed: 0,
            index_grows_started: 0,
            index_grows_completed: 0,
            index_grows_failed: 0,
            recoveries_started: 0,
            recoveries_failed: 0,
            pending_io_submitted: 0,
            pending_io_completed: 0,
            pending_io_failed: 0,
        };
        let renderer = PrometheusRenderer::new();
        let render = Arc::new(move || renderer.render_snapshot(&snapshot));

        let server = MetricsHttpServer::bind("127.0.0.1:0".parse().unwrap(), render)
            .await
            .unwrap();
        let addr = server.local_addr();

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut resp))
            .await
            .unwrap()
            .unwrap();
        let resp = String::from_utf8_lossy(&resp);
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("oxifaster_operations_total 1"));

        server.shutdown().await.unwrap();
    }
}
