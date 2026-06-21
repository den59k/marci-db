include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b2_b7_read_once_marci  BENCH_SCALE={n}  (Place full scan)");

    let marci = MarciTcpClient::new();

    for i in 0..n {
        marci.insert("Place", &json!({
            "name":              format!("Bench Place {}", i),
            "description":       "desc",
            "address":           format!("{} St", i),
            "type":              "food",
            "averageBill":       800i64,
            "deliveryAvailable": true,
            "openingHours":      "10:00-22:00"
        }));
    }

    let monitor = ResourceMonitor::with_warm_up(ServerKind::MarciDB, Duration::ZERO);

    let elapsed = monitor.measure_fn(
        &format!("Place find_many ×{n} records (MarciDB TCP/MDWP → CanopyDB)"),
        || {
            let t = Instant::now();
            marci.find_many("Place", &json!({ "id": true }));
            t.elapsed()
        },
    );

    eprintln!("[timing] Place find_many ({n} records): {elapsed:.2?}");
}