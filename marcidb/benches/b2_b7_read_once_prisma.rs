include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b2_b7_read_once_prisma  BENCH_SCALE={n}  (Place full scan)");

    restart_postgres();
    let pg = PrismaEngineClient::new();
    pg.reset_data();

    let records: Vec<Value> = (0..n).map(|i| json!({
        "name":              format!("Bench Place {}", i),
        "description":       "desc",
        "address":           format!("{} St", i),
        "type":              "food",
        "averageBill":       800i64,
        "deliveryAvailable": true,
        "openingHours":      "10:00-22:00"
    })).collect();
    pg.create_many_place(&records);

    let monitor = ResourceMonitor::with_warm_up(ServerKind::Postgres, Duration::ZERO);

    let elapsed = monitor.measure_fn(
        &format!("Place find_many ×{n} records (Prisma → PostgreSQL)"),
        || {
            let t = Instant::now();
            pg.find_many_place(&json!({ "id": true }));
            t.elapsed()
        },
    );

    eprintln!("[timing] Place find_many ({n} records): {elapsed:.2?}");
}