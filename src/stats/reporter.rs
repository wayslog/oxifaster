//! Statistics reporter
//!
//! Provides formatting and output for statistics.

use std::fmt::Write;

use crate::stats::collector::{StatsCollector, StatsSnapshot};

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

        writeln!(output, "=== FASTER Statistics ===").unwrap();
        writeln!(output, "Elapsed: {:.2?}", snapshot.elapsed).unwrap();
        writeln!(output).unwrap();

        writeln!(output, "Operations:").unwrap();
        writeln!(output, "  Total:    {}", snapshot.total_operations).unwrap();
        writeln!(
            output,
            "  Reads:    {} (hits: {}, rate: {:.2}%)",
            snapshot.reads,
            snapshot.read_hits,
            snapshot.hit_rate * 100.0
        )
        .unwrap();
        writeln!(output, "  Upserts:  {}", snapshot.upserts).unwrap();
        writeln!(output, "  RMWs:     {}", snapshot.rmws).unwrap();
        writeln!(output, "  Deletes:  {}", snapshot.deletes).unwrap();
        writeln!(output, "  Pending:  {}", snapshot.pending).unwrap();
        writeln!(output).unwrap();

        writeln!(output, "Performance:").unwrap();
        writeln!(output, "  Throughput: {:.2} ops/sec", snapshot.throughput).unwrap();
        writeln!(output, "  Avg Latency: {:.2?}", snapshot.avg_latency).unwrap();
        writeln!(output).unwrap();

        if self.detailed {
            writeln!(output, "Index:").unwrap();
            writeln!(output, "  Entries: {}", snapshot.index_entries).unwrap();
            writeln!(
                output,
                "  Load Factor: {:.2}%",
                snapshot.index_load_factor * 100.0
            )
            .unwrap();
            writeln!(output).unwrap();

            writeln!(output, "Memory:").unwrap();
            writeln!(
                output,
                "  Allocated: {}",
                StatsSnapshot::format_bytes(snapshot.bytes_allocated)
            )
            .unwrap();
            writeln!(
                output,
                "  In Use: {}",
                StatsSnapshot::format_bytes(snapshot.memory_in_use)
            )
            .unwrap();
            writeln!(
                output,
                "  Peak: {}",
                StatsSnapshot::format_bytes(snapshot.peak_memory)
            )
            .unwrap();
            writeln!(output).unwrap();

            writeln!(output, "I/O:").unwrap();
            writeln!(output, "  Pages Flushed: {}", snapshot.pages_flushed).unwrap();
        }

        output
    }

    fn format_json(&self, snapshot: &StatsSnapshot) -> String {
        let mut output = String::new();

        writeln!(output, "{{").unwrap();
        writeln!(
            output,
            "  \"elapsed_ms\": {},",
            snapshot.elapsed.as_millis()
        )
        .unwrap();
        writeln!(output, "  \"operations\": {{").unwrap();
        writeln!(output, "    \"total\": {},", snapshot.total_operations).unwrap();
        writeln!(output, "    \"reads\": {},", snapshot.reads).unwrap();
        writeln!(output, "    \"read_hits\": {},", snapshot.read_hits).unwrap();
        writeln!(output, "    \"upserts\": {},", snapshot.upserts).unwrap();
        writeln!(output, "    \"rmws\": {},", snapshot.rmws).unwrap();
        writeln!(output, "    \"deletes\": {},", snapshot.deletes).unwrap();
        writeln!(output, "    \"pending\": {}", snapshot.pending).unwrap();
        writeln!(output, "  }},").unwrap();
        writeln!(output, "  \"performance\": {{").unwrap();
        writeln!(output, "    \"throughput\": {:.2},", snapshot.throughput).unwrap();
        writeln!(output, "    \"hit_rate\": {:.4},", snapshot.hit_rate).unwrap();
        writeln!(
            output,
            "    \"avg_latency_ns\": {}",
            snapshot.avg_latency.as_nanos()
        )
        .unwrap();
        writeln!(output, "  }},").unwrap();
        writeln!(output, "  \"index\": {{").unwrap();
        writeln!(output, "    \"entries\": {},", snapshot.index_entries).unwrap();
        writeln!(
            output,
            "    \"load_factor\": {:.4}",
            snapshot.index_load_factor
        )
        .unwrap();
        writeln!(output, "  }},").unwrap();
        writeln!(output, "  \"memory\": {{").unwrap();
        writeln!(
            output,
            "    \"bytes_allocated\": {},",
            snapshot.bytes_allocated
        )
        .unwrap();
        writeln!(output, "    \"in_use\": {},", snapshot.memory_in_use).unwrap();
        writeln!(output, "    \"peak\": {}", snapshot.peak_memory).unwrap();
        writeln!(output, "  }},").unwrap();
        writeln!(output, "  \"io\": {{").unwrap();
        writeln!(output, "    \"pages_flushed\": {}", snapshot.pages_flushed).unwrap();
        writeln!(output, "  }}").unwrap();
        writeln!(output, "}}").unwrap();

        output
    }

    fn format_csv(&self, snapshot: &StatsSnapshot) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{:.2},{:.4},{},{},{},{},{}",
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
            snapshot.pages_flushed
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
        "elapsed_ms,total_ops,reads,read_hits,upserts,rmws,deletes,pending,throughput,hit_rate,index_entries,bytes_allocated,memory_in_use,peak_memory,pages_flushed"
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
        assert_eq!(values.len(), 15);
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
        assert_eq!(fields.len(), 15);
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
