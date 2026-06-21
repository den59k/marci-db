include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b2_insert_once_marci  BENCH_SCALE={n}");

    eprintln!(
        "\n\
         ╔══════════════════════════════════════════════════════════════╗\n\
         ║  b2_insert_once_marci — подготовка                          ║\n\
         ╠══════════════════════════════════════════════════════════════╣\n\
         ║  Сбросьте MarciDB:                                           ║\n\
         ║    1. Остановите сервер (Ctrl+C)                             ║\n\
         ║    2. Удалите папку data                                     ║\n\
         ║    3. cargo run --release --bin marcidb-server               ║\n\
         ╚══════════════════════════════════════════════════════════════╝"
    );
    eprint!("  Нажмите Enter, когда сервер запущен с чистой БД... ");
    { use std::io::BufRead; std::io::stdin().lock().lines().next(); }

    let marci   = MarciTcpClient::new();
    let monitor = ResourceMonitor::with_warm_up(ServerKind::MarciDB, Duration::ZERO);

    let place_records: Vec<Value> = (0..n).map(|i| json!({
        "name": format!("Bench Food {i}"), "description": "desc",
        "address": format!("{i} St"), "type": "food",
        "averageBill": 800i64, "deliveryAvailable": true,
        "openingHours": "10:00-22:00"
    })).collect();

    let elapsed = monitor.measure_fn(
        &format!("Place ×{n} (MarciDB)"),
        || {
            let t = Instant::now();
            for r in &place_records { marci.insert("Place", r); }
            t.elapsed()
        },
    );

    eprintln!("[timing] Place ×{n}: {elapsed:.2?}");
    eprintln!("[disk] Подождите ~5 с, затем проверьте размер data/ вручную.");
}