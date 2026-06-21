include!("_bench_common.rs");

fn main() {
    // Размер таблицы: достаточно большой, чтобы запрос был нетривиальным,
    // но не настолько большой, чтобы засев занимал много времени.
    const SEED_SIZE: usize = 10000;

    const LEVELS: &[usize] = &[1, 5, 10, 20, 30, 50];

    for &n in LEVELS {
        eprintln!(
            "\n\
             ╔══════════════════════════════════════════════════════════════╗\n\
             ║  b9_concurrent_once_marci — УРОВЕНЬ КОНКУРЕНТНОСТИ n={n:<3}    ║\n\
             ╠══════════════════════════════════════════════════════════════╣\n\
             ║  Необходимо сбросить состояние MarciDB:                      ║\n\
             ║    1. Остановите MarciDB-сервер (Ctrl+C)                     ║\n\
             ║    2. rm -rf ./data                                           ║\n\
             ║    3. cargo run --release --bin marcidb-server               ║\n\
             ╚══════════════════════════════════════════════════════════════╝"
        );
        eprint!("  Нажмите Enter, когда MarciDB-сервер запущен с чистой БД... ");
        { use std::io::BufRead; std::io::stdin().lock().lines().next(); }

        let marci_addr = std::env::var("MARCIDB_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n.max(2))
            .enable_all()
            .build()
            .unwrap();
        let conns: Arc<Vec<Arc<tokio::sync::Mutex<MarciConn>>>> = Arc::new(
            rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(Arc::new(tokio::sync::Mutex::new(
                        MarciConn::connect(&marci_addr).await,
                    )));
                }
                v
            }),
        );

        // Засеваем SEED_SIZE записей Place.
        // 1 запись даёт моментальный результат и не отражает реальную нагрузку;
        // 1000 записей обеспечивают измеримое время сканирования таблицы.
        eprintln!("[bench] n={n}: засеваем {SEED_SIZE} записей Place...");
        rt.block_on(async {
            for i in 0..SEED_SIZE {
                let frame = mdwp_insert(
                    "Place",
                    &serde_json::to_vec(&json!({
                        "name":        format!("Seed_{}", i),
                        "description": "seed",
                        "address":     format!("{} St", i),
                        "type":        "food",
                        "averageBill": 500i64
                    })).unwrap(),
                );
                conns[0].lock().await.roundtrip(frame).await;
            }
        });
        eprintln!("[bench] n={n}: засев завершён ({SEED_SIZE} записей Place).");

        let monitor = ResourceMonitor::with_warm_up(ServerKind::MarciDB, Duration::ZERO);

        let elapsed = monitor.measure_fn(
            &format!("Place READ ×{n} concurrent (MarciDB find_many TCP/MDWP → CanopyDB, {SEED_SIZE} records)"),
            || {
                let t = Instant::now();
                rt.block_on(async {
                    let mut hs = Vec::with_capacity(n);
                    for j in 0..n {
                        let conn = conns[j % conns.len()].clone();
                        hs.push(tokio::spawn(async move {
                            let frame = mdwp_find_many(
                                "Place",
                                &serde_json::to_vec(&json!({ "id": true })).unwrap(),
                            );
                            conn.lock().await.roundtrip(frame).await;
                        }));
                    }
                    for h in hs { h.await.unwrap(); }
                });
                t.elapsed()
            },
        );

        eprintln!("[timing] n={n} READ ({SEED_SIZE} records): {elapsed:.2?}");
    }
}