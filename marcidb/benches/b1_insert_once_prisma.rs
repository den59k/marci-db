include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b2_insert_once_prisma  BENCH_SCALE={n}");

    restart_postgres();
    let pg      = PrismaEngineClient::new();
    let monitor = ResourceMonitor::with_warm_up(ServerKind::Postgres, Duration::ZERO);

    pg.reset_data();

    let place_records: Vec<Value> = (0..n).map(|i| json!({
        "name": format!("Bench Food {i}"), "description": "desc",
        "address": format!("{i} St"), "type": "food",
        "averageBill": 800i64, "deliveryAvailable": true,
        "openingHours": "10:00-22:00"
    })).collect();

    let elapsed = monitor.measure_fn(
        &format!("Place ×{n} (Prisma)"),
        || {
            let t = Instant::now();
            pg.create_many_place(&place_records);
            t.elapsed()
        },
    );

    eprintln!("[timing] Place ×{n}: {elapsed:.2?}");
    report_pg_db_size();
}