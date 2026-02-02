//! Statistics reporter
//!
//! Provides formatting and output for statistics.

use std::fmt::{Arguments, Write};

use crate::stats::collector::{StatsCollector, StatsSnapshot};

fn push_line(output: &mut String, args: Arguments) {
    let _ = output.write_fmt(args);
    output.push('\n');
}

/// Output format for statistics reports
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    /// Human-readable text format
    Text,
    /// JSON format
    Json,
    /// CSV format (for time series)
    Csv,
    /// Compact one-line format
    Compact,
}

/// Statistics reporter
///
/// Formats and outputs statistics in various formats.
pub struct StatsReporter {
    /// Output format
    format: ReportFormat,
    /// Whether to include detailed stats
    detailed: bool,
}

impl StatsReporter {
    /// Create a new reporter with the given format
    pub fn new(format: ReportFormat) -> Self {
        Self {
            format,
            detailed: true,
        }
    }

    /// Create a reporter with text format
    pub fn text() -> Self {
        Self::new(ReportFormat::Text)
    }

    /// Create a reporter with JSON format
    pub fn json() -> Self {
        Self::new(ReportFormat::Json)
    }

    /// Set whether to include detailed stats
    pub fn with_detailed(mut self, detailed: bool) -> Self {
        self.detailed = detailed;
        self
    }

    /// Generate a report from a collector
    pub fn report(&self, collector: &StatsCollector) -> String {
        let snapshot = collector.snapshot();
        self.report_snapshot(&snapshot)
    }

    /// Generate a report from a snapshot
    pub fn report_snapshot(&self, snapshot: &StatsSnapshot) -> String {
        match self.format {
            ReportFormat::Text => self.format_text(snapshot),
            ReportFormat::Json => self.format_json(snapshot),
            ReportFormat::Csv => self.format_csv(snapshot),
            ReportFormat::Compact => self.format_compact(snapshot),
        }
    }

    fn format_text(&self, snapshot: &StatsSnapshot) -> String {
        let mut output = String::new();

        push_line(&mut output, format_args!("=== FASTER Statistics ==="));
        push_line(
            &mut output,
            format_args!("Elapsed: {:.2?}", snapshot.elapsed),
        );
        output.push('\n');

        push_line(&mut output, format_args!("Operations:"));
        push_line(
            &mut output,
            format_args!("  Total:    {}", snapshot.total_operations),
        );
        push_line(
            &mut output,
            format_args!(
                "  Reads:    {} (hits: {}, rate: {:.2}%)",
                snapshot.reads,
                snapshot.read_hits,
                snapshot.hit_rate * 100.0
            ),
        );
        push_line(
            &mut output,
            format_args!("  Upserts:  {}", snapshot.upserts),
        );
        push_line(&mut output, format_args!("  RMWs:     {}", snapshot.rmws));
        push_line(
            &mut output,
            format_args!("  Deletes:  {}", snapshot.deletes),
        );
        push_line(
            &mut output,
            format_args!("  Pending:  {}", snapshot.pending),
        );
        output.push('\n');

        push_line(&mut output, format_args!("Performance:"));
        push_line(
            &mut output,
            format_args!("  Throughput: {:.2} ops/sec", snapshot.throughput),
        );
        push_line(
            &mut output,
            format_args!("  Avg Latency: {:.2?}", snapshot.avg_latency),
        );
        output.push('\n');

        if self.detailed {
            push_line(&mut output, format_args!("Index:"));
            push_line(
                &mut output,
                format_args!("  Entries: {}", snapshot.index_entries),
            );
            push_line(
                &mut output,
                format_args!("  Load Factor: {:.2}%", snapshot.index_load_factor * 100.0),
            );
            output.push('\n');

            push_line(&mut output, format_args!("Memory:"));
            push_line(
                &mut output,
                format_args!(
                    "  Allocated: {}",
                    StatsSnapshot::format_bytes(snapshot.bytes_allocated)
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  In Use: {}",
                    StatsSnapshot::format_bytes(snapshot.memory_in_use)
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  Peak: {}",
                    StatsSnapshot::format_bytes(snapshot.peak_memory)
                ),
            );
            output.push('\n');

            push_line(&mut output, format_args!("I/O:"));
            push_line(
                &mut output,
                format_args!("  Pages Flushed: {}", snapshot.pages_flushed),
            );
            output.push('\n');

            push_line(&mut output, format_args!("Maintenance:"));
            push_line(
                &mut output,
                format_args!(
                    "  Checkpoints: {} started / {} ok / {} failed",
                    snapshot.checkpoints_started,
                    snapshot.checkpoints_completed,
                    snapshot.checkpoints_failed
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  Compactions: {} started / {} ok / {} failed",
                    snapshot.compactions_started,
                    snapshot.compactions_completed,
                    snapshot.compactions_failed
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  Index Growth: {} started / {} ok / {} failed",
                    snapshot.index_grows_started,
                    snapshot.index_grows_completed,
                    snapshot.index_grows_failed
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  Recovery: {} started / {} failed",
                    snapshot.recoveries_started, snapshot.recoveries_failed
                ),
            );
            push_line(
                &mut output,
                format_args!(
                    "  Pending I/O: {} submitted / {} completed / {} failed",
                    snapshot.pending_io_submitted,
                    snapshot.pending_io_completed,
                    snapshot.pending_io_failed
                ),
            );
        }

        output
    }

    fn format_json(&self, snapshot: &StatsSnapshot) -> String {
        let mut output = String::new();

        push_line(&mut output, format_args!("{{"));
        push_line(
            &mut output,
            format_args!("  \"elapsed_ms\": {},", snapshot.elapsed.as_millis()),
        );
        push_line(&mut output, format_args!("  \"operations\": {{"));
        push_line(
            &mut output,
            format_args!("    \"total\": {},", snapshot.total_operations),
        );
        push_line(
            &mut output,
            format_args!("    \"reads\": {},", snapshot.reads),
        );
        push_line(
            &mut output,
            format_args!("    \"read_hits\": {},", snapshot.read_hits),
        );
        push_line(
            &mut output,
            format_args!("    \"upserts\": {},", snapshot.upserts),
        );
        push_line(
            &mut output,
            format_args!("    \"rmws\": {},", snapshot.rmws),
        );
        push_line(
            &mut output,
            format_args!("    \"deletes\": {},", snapshot.deletes),
        );
        push_line(
            &mut output,
            format_args!("    \"pending\": {}", snapshot.pending),
        );
        push_line(&mut output, format_args!("  }},"));
        push_line(&mut output, format_args!("  \"performance\": {{"));
        push_line(
            &mut output,
            format_args!("    \"throughput\": {:.2},", snapshot.throughput),
        );
        push_line(
            &mut output,
            format_args!("    \"hit_rate\": {:.4},", snapshot.hit_rate),
        );
        push_line(
            &mut output,
            format_args!(
                "    \"avg_latency_ns\": {}",
                snapshot.avg_latency.as_nanos()
            ),
        );
        push_line(&mut output, format_args!("  }},"));
        push_line(&mut output, format_args!("  \"index\": {{"));
        push_line(
            &mut output,
            format_args!("    \"entries\": {},", snapshot.index_entries),
        );
        push_line(
            &mut output,
            format_args!("    \"load_factor\": {:.4}", snapshot.index_load_factor),
        );
        push_line(&mut output, format_args!("  }},"));
        push_line(&mut output, format_args!("  \"memory\": {{"));
        push_line(
            &mut output,
            format_args!("    \"bytes_allocated\": {},", snapshot.bytes_allocated),
        );
        push_line(
            &mut output,
            format_args!("    \"in_use\": {},", snapshot.memory_in_use),
        );
        push_line(
            &mut output,
            format_args!("    \"peak\": {}", snapshot.peak_memory),
        );
        push_line(&mut output, format_args!("  }},"));
        push_line(&mut output, format_args!("  \"io\": {{"));
        push_line(
            &mut output,
            format_args!("    \"pages_flushed\": {}", snapshot.pages_flushed),
        );
        push_line(&mut output, format_args!("  }},"));
        push_line(&mut output, format_args!("  \"maintenance\": {{"));
        push_line(
            &mut output,
            format_args!(
                "    \"checkpoints\": {{ \"started\": {}, \"completed\": {}, \"failed\": {} }},",
                snapshot.checkpoints_started,
                snapshot.checkpoints_completed,
                snapshot.checkpoints_failed
            ),
        );
        push_line(
            &mut output,
            format_args!(
                "    \"compactions\": {{ \"started\": {}, \"completed\": {}, \"failed\": {} }},",
                snapshot.compactions_started,
                snapshot.compactions_completed,
                snapshot.compactions_failed
            ),
        );
        push_line(
            &mut output,
            format_args!(
                "    \"index_growth\": {{ \"started\": {}, \"completed\": {}, \"failed\": {} }},",
                snapshot.index_grows_started,
                snapshot.index_grows_completed,
                snapshot.index_grows_failed
            ),
        );
        push_line(
            &mut output,
            format_args!(
                "    \"recovery\": {{ \"started\": {}, \"failed\": {} }},",
                snapshot.recoveries_started, snapshot.recoveries_failed
            ),
        );
        push_line(
            &mut output,
            format_args!(
                "    \"pending_io\": {{ \"submitted\": {}, \"completed\": {}, \"failed\": {} }}",
                snapshot.pending_io_submitted,
                snapshot.pending_io_completed,
                snapshot.pending_io_failed
            ),
        );
        push_line(&mut output, format_args!("  }}"));
        push_line(&mut output, format_args!("}}"));

        output
    }

    fn format_csv(&self, snapshot: &StatsSnapshot) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{:.2},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            snapshot.elapsed.as_millis(),
            snapshot.total_operations,
            snapshot.reads,
            snapshot.read_hits,
            snapshot.upserts,
            snapshot.rmws,
            snapshot.deletes,
            snapshot.pending,
            snapshot.throughput,
            snapshot.hit_rate,
            snapshot.index_entries,
            snapshot.bytes_allocated,
            snapshot.memory_in_use,
            snapshot.peak_memory,
            snapshot.pages_flushed,
            snapshot.checkpoints_started,
            snapshot.checkpoints_completed,
            snapshot.checkpoints_failed,
            snapshot.compactions_started,
            snapshot.compactions_completed,
            snapshot.compactions_failed,
            snapshot.index_grows_started,
            snapshot.index_grows_completed,
            snapshot.index_grows_failed,
            snapshot.recoveries_started,
            snapshot.recoveries_failed,
            snapshot.pending_io_submitted,
            snapshot.pending_io_completed,
            snapshot.pending_io_failed
        )
    }

    fn format_compact(&self, snapshot: &StatsSnapshot) -> String {
        format!(
            "ops={} tput={:.0}/s hit={:.1}% lat={:.2?} mem={}",
            snapshot.total_operations,
            snapshot.throughput,
            snapshot.hit_rate * 100.0,
            snapshot.avg_latency,
            StatsSnapshot::format_bytes(snapshot.memory_in_use)
        )
    }

    /// Get CSV header line
    pub fn csv_header() -> &'static str {
        "elapsed_ms,total_ops,reads,read_hits,upserts,rmws,deletes,pending,throughput,hit_rate,index_entries,bytes_allocated,memory_in_use,peak_memory,pages_flushed,checkpoints_started,checkpoints_completed,checkpoints_failed,compactions_started,compactions_completed,compactions_failed,index_grows_started,index_grows_completed,index_grows_failed,recoveries_started,recoveries_failed,pending_io_submitted,pending_io_completed,pending_io_failed"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_snapshot() -> StatsSnapshot {
        StatsSnapshot {
            elapsed: Duration::from_secs(10),
            total_operations: 1000,
            reads: 500,
            read_hits: 400,
            upserts: 300,
            rmws: 150,
            deletes: 50,
            conditional_inserts: 0,
            conditional_insert_exists: 0,
            pending: 5,
            hit_rate: 0.8,
            throughput: 100.0,
            avg_latency: Duration::from_micros(50),
            index_entries: 10000,
            index_load_factor: 0.5,
            bytes_allocated: 1048576,
            pages_flushed: 10,
            memory_in_use: 524288,
            peak_memory: 1048576,
            checkpoints_started: 2,
            checkpoints_completed: 2,
            checkpoints_failed: 0,
            compactions_started: 1,
            compactions_completed: 1,
            compactions_failed: 0,
            index_grows_started: 1,
            index_grows_completed: 1,
            index_grows_failed: 0,
            recoveries_started: 1,
            recoveries_failed: 0,
            pending_io_submitted: 10,
            pending_io_completed: 9,
            pending_io_failed: 1,
        }
    }

    #[test]
    fn test_text_format() {
        let reporter = StatsReporter::text();
        let snapshot = sample_snapshot();
        let output = reporter.report_snapshot(&snapshot);

        assert!(output.contains("FASTER Statistics"));
        assert!(output.contains("Total:    1000"));
        assert!(output.contains("Reads:    500"));
        assert!(output.contains("Throughput: 100.00 ops/sec"));
    }

    #[test]
    fn test_json_format() {
        let reporter = StatsReporter::json();
        let snapshot = sample_snapshot();
        let output = reporter.report_snapshot(&snapshot);

        assert!(output.contains("\"total\": 1000"));
        assert!(output.contains("\"throughput\": 100.00"));
        assert!(output.contains("\"hit_rate\": 0.8000"));
    }

    #[test]
    fn test_csv_format() {
        let reporter = StatsReporter::new(ReportFormat::Csv);
        let snapshot = sample_snapshot();
        let output = reporter.report_snapshot(&snapshot);

        // CSV should be a single line with comma-separated values
        assert!(!output.contains('\n'));
        let values: Vec<&str> = output.split(',').collect();
        assert_eq!(values.len(), 29);
    }

    #[test]
    fn test_compact_format() {
        let reporter = StatsReporter::new(ReportFormat::Compact);
        let snapshot = sample_snapshot();
        let output = reporter.report_snapshot(&snapshot);

        assert!(output.contains("ops=1000"));
        assert!(output.contains("tput=100/s"));
        assert!(output.contains("hit=80.0%"));
    }

    #[test]
    fn test_csv_header() {
        let header = StatsReporter::csv_header();
        let fields: Vec<&str> = header.split(',').collect();
        assert_eq!(fields.len(), 29);
        assert!(header.contains("elapsed_ms"));
        assert!(header.contains("throughput"));
    }

    #[test]
    fn test_detailed_toggle() {
        let reporter = StatsReporter::text().with_detailed(false);
        let snapshot = sample_snapshot();
        let output = reporter.report_snapshot(&snapshot);

        // Should not contain detailed sections
        assert!(!output.contains("Index:"));
        assert!(!output.contains("Memory:"));
    }
}
