//! Prometheus text exposition example (public API).
//!
//! Run:
//!   cargo run --example prometheus_metrics --features prometheus

#[cfg(not(feature = "prometheus"))]
fn main() {
    eprintln!(
        "This example requires the `prometheus` feature. Run:\n  cargo run --example prometheus_metrics --features prometheus"
    );
}

#[cfg(feature = "prometheus")]
fn main() -> std::io::Result<()> {
    use std::sync::Arc;

    use oxifaster::device::NullDisk;
    use oxifaster::stats::prometheus::MetricsHttpServer;
    use oxifaster::stats::prometheus::PrometheusRenderer;
    use oxifaster::status::Status;
    use oxifaster::store::{FasterKv, FasterKvConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
        match r {
            Ok(v) => v,
            Err(status) => panic!("{ctx} failed: {status:?}"),
        }
    }

    let store = Arc::new(FasterKv::<u64, u64, _>::new(
        FasterKvConfig::default(),
        NullDisk::new(),
    ));
    let mut session = expect_ok("start_session", store.start_session());
    session.upsert(1, 10);
    session.upsert(2, 20);
    assert_eq!(expect_ok("read(1)", session.read(&1)), Some(10));

    let snapshot = store.stats_snapshot();
    assert!(snapshot.total_operations >= 3);

    let text = PrometheusRenderer::new().render_snapshot(&snapshot);
    println!("{text}");

    // Ensure a few key metrics are present for external observability.
    assert!(text.contains("oxifaster_operations_total"));
    assert!(text.contains("oxifaster_reads_total"));
    assert!(text.contains("oxifaster_uptime_seconds"));

    // Start a minimal HTTP server and fetch `/metrics`.
    let store_for_server = store.clone();
    let render = Arc::new(move || {
        PrometheusRenderer::new().render_snapshot(&store_for_server.stats_snapshot())
    });

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async move {
        let server = MetricsHttpServer::bind("127.0.0.1:0".parse().unwrap(), render)
            .await
            .expect("failed to bind metrics server");
        let addr = server.local_addr();

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("failed to connect to metrics server");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("failed to write request");

        let mut resp = Vec::new();
        stream
            .read_to_end(&mut resp)
            .await
            .expect("failed to read response");
        let resp = String::from_utf8_lossy(&resp);
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("oxifaster_operations_total"));

        server.shutdown().await.expect("failed to shutdown server");
    });

    Ok(())
}
