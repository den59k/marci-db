include!("_bench_common.rs");

fn bench_concurrent_marci(c: &mut Criterion) {
    const LEVELS:    &[usize] = &[1, 5, 10, 20, 30, 50];
    // Фиксированный размер таблицы для READ / UPDATE.
    // Не зависит от уровня конкурентности — условия одинаковы для всех n.
    const SEED_SIZE: usize = 1000;

    for &n in LEVELS {
        // ── Пауза для ручного сброса MarciDB ─────────────────────────────────
        //
        //  MarciDB не имеет команды сброса данных — пользователь обязан:
        //    1. Остановить MarciDB-сервер (Ctrl+C)
        //    2. rm -rf ./data
        //    3. cargo run --release --bin marcidb-server
        //  Только после этого нажать Enter, чтобы бенчмарк продолжился.
        //
        //  Перезапуск между операциями внутри одного уровня НЕ нужен:
        //    - цель — сравнение систем в одинаковых (тёплых) условиях кэша;
        //    - размер таблицы контролируется программно (очистка + засев).

        eprintln!(
            "\n\
             ╔══════════════════════════════════════════════════════════════╗\n\
             ║      B9 MarciDB — ПОДГОТОВКА К УРОВНЮ КОНКУРЕНТНОСТИ n={n:<3} ║\n\
             ╠══════════════════════════════════════════════════════════════╣\n\
             ║  Необходимо сбросить состояние MarciDB:                      ║\n\
             ║    1. Остановите MarciDB-сервер (Ctrl+C)                     ║\n\
             ║    2. rm -rf ./data                                           ║\n\
             ║    3. cargo run --release --bin marcidb-server               ║\n\
             ╚══════════════════════════════════════════════════════════════╝"
        );
        eprint!("  Нажмите Enter, когда MarciDB-сервер запущен с чистой БД... ");
        {
            use std::io::BufRead;
            std::io::stdin().lock().lines().next();
        }
        eprintln!("  [bench] Продолжаем. Запускаем уровень конкурентности n={n}.\n");

        let mut group = c.benchmark_group(format!("B9_concurrent_marci/concurrency={n}"));
        group.throughput(Throughput::Elements(n as u64));

        // ── MarciDB: пул из N независимых TCP-соединений ──────────────────────
        //
        //  Каждая tokio-задача получает своё выделенное соединение,
        //  что исключает сериализацию через Mutex (честный параллелизм).

        let marci_addr = std::env::var("MARCIDB_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let marci_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(n.max(2))
                .enable_all()
                .build()
                .unwrap(),
        );
        let marci_conns: Arc<Vec<Arc<tokio::sync::Mutex<MarciConn>>>> = Arc::new(
            marci_rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(Arc::new(tokio::sync::Mutex::new(
                        MarciConn::connect(&marci_addr).await,
                    )));
                }
                v
            }),
        );

        // ═════════════════════════════════════════════════════════════════════
        //  INSERT: N параллельных вставок Place
        //
        //  Предварительный засев не нужен: бенчмарк сам создаёт записи.
        //  После окончания INSERT-бенчмарка таблица очищается и засевается
        //  ровно SEED_SIZE записями для READ и UPDATE (см. ниже).
        // ═════════════════════════════════════════════════════════════════════

        let ins_ctr = Arc::new(AtomicU64::new(0u64));

        {
            let marci_rt2 = marci_rt.clone();
            let conns     = marci_conns.clone();
            let ctr       = ins_ctr.clone();

            group.bench_function(
                format!("TO-BE / Place INSERT ×{n} (MarciDB TCP/MDWP → CanopyDB)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let base    = ctr.fetch_add(n as u64, Ordering::Relaxed);
                            let elapsed = marci_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for j in 0..n {
                                    let conn = conns[j].clone();
                                    let idx  = base + j as u64;
                                    hs.push(tokio::spawn(async move {
                                        let frame = mdwp_insert(
                                            "Place",
                                            &serde_json::to_vec(&json!({
                                                "name":        format!("ConcIns marci {}", idx),
                                                "description": "desc",
                                                "address":     "1 St",
                                                "type":        "food",
                                                "averageBill": 800i64
                                            }))
                                                .unwrap(),
                                        );
                                        conn.lock().await.roundtrip(frame).await;
                                    }));
                                }
                                for h in hs { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    })
                },
            );
        }

        // ── Очистка после INSERT + засев SEED_SIZE записей ────────────────────
        //
        //  Criterion выполняет десятки итераций, каждая вставляет N записей.
        //  После окончания INSERT-бенчмарка в таблице тысячи строк — сбрасываем
        //  MarciDB вручную, затем засеваем ровно SEED_SIZE записей.
        //
        //  Рестарт не создаёт дисбаланса с PostgreSQL: Criterion перед каждым
        //  измерением делает warm-up (3 с), за которые кэш обеих СУБД прогревается
        //  одинаково — к моменту замеров обе работают на тёплых данных.

        eprintln!(
            "\n\
             ╔══════════════════════════════════════════════════════════════╗\n\
             ║  INSERT завершён — необходимо сбросить MarciDB (n={n:<3})      ║\n\
             ╠══════════════════════════════════════════════════════════════╣\n\
             ║  Таблица засорена данными Criterion. Сбросьте MarciDB:       ║\n\
             ║    1. Остановите MarciDB-сервер (Ctrl+C)                     ║\n\
             ║    2. rm -rf ./data                                           ║\n\
             ║    3. cargo run --release --bin marcidb-server               ║\n\
             ╚══════════════════════════════════════════════════════════════╝"
        );
        eprint!("  Нажмите Enter, когда MarciDB-сервер запущен с чистой БД... ");
        {
            use std::io::BufRead;
            std::io::stdin().lock().lines().next();
        }

        // Пересоздаём пул соединений — старые привязаны к убитому серверу.
        let marci_conns: Arc<Vec<Arc<tokio::sync::Mutex<MarciConn>>>> = Arc::new(
            marci_rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(Arc::new(tokio::sync::Mutex::new(
                        MarciConn::connect(&marci_addr).await,
                    )));
                }
                v
            }),
        );

        eprintln!("[bench] n={n}: засеваем {SEED_SIZE} записей Place для READ/UPDATE...");

        let marci_seed_ids: Arc<Vec<Value>> = Arc::new(marci_rt.block_on(async {
            let mut ids = Vec::with_capacity(SEED_SIZE);
            for i in 0..SEED_SIZE {
                let frame = mdwp_insert(
                    "Place",
                    &serde_json::to_vec(&json!({
                        "name":        format!("ReadSeed_n{}_r{}", n, i),
                        "description": "seed",
                        "address":     "seed addr",
                        "type":        "food",
                        "averageBill": 500i64
                    })).unwrap(),
                );
                let resp = marci_conns[0].lock().await.roundtrip(frame).await;
                let s    = String::from_utf8(resp).unwrap();
                let val: Value = serde_json::from_str(&s).unwrap_or(Value::String(s));
                ids.push(val);
            }
            ids
        }));

        eprintln!("[bench] n={n}: засев завершён ({SEED_SIZE} записей Place).");

        // ═════════════════════════════════════════════════════════════════════
        //  READ: N параллельных find_many (полный скан Place)
        //
        //  Таблица содержит ровно SEED_SIZE записей — детерминированная нагрузка.
        // ═════════════════════════════════════════════════════════════════════

        {
            let marci_rt2 = marci_rt.clone();
            let conns     = marci_conns.clone();

            group.bench_function(
                format!("TO-BE / Place READ ×{n} (MarciDB find_many TCP/MDWP → CanopyDB)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = marci_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for j in 0..n {
                                    let conn = conns[j].clone();
                                    hs.push(tokio::spawn(async move {
                                        let frame = mdwp_find_many(
                                            "Place",
                                            &serde_json::to_vec(&json!({ "id": true }))
                                                .unwrap(),
                                        );
                                        conn.lock().await.roundtrip(frame).await;
                                    }));
                                }
                                for h in hs { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    })
                },
            );
        }

        // ═════════════════════════════════════════════════════════════════════
        //  UPDATE: N параллельных обновлений seeded Place-записей
        //
        //  marci_seed_ids содержит SEED_SIZE ID-ов; используем [j % SEED_SIZE],
        //  чтобы корректно работать при любом n ≤ SEED_SIZE.
        // ═════════════════════════════════════════════════════════════════════

        {
            let marci_rt2 = marci_rt.clone();
            let conns     = marci_conns.clone();
            let marci_ids = marci_seed_ids.clone();

            group.bench_function(
                format!("TO-BE / Place UPDATE ×{n} (MarciDB TCP/MDWP → CanopyDB)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = marci_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for j in 0..n {
                                    let conn   = conns[j].clone();
                                    let id_url = marci_value_to_id_url(&marci_ids[j % SEED_SIZE]);
                                    hs.push(tokio::spawn(async move {
                                        let frame = mdwp_update(
                                            "Place",
                                            &id_url,
                                            &serde_json::to_vec(
                                                &json!({ "description": "ConcUpdated" })
                                            ).unwrap(),
                                        );
                                        conn.lock().await.roundtrip(frame).await;
                                    }));
                                }
                                for h in hs { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    })
                },
            );
        }

        // ═════════════════════════════════════════════════════════════════════
        //  DELETE: N параллельных удалений
        //
        //  setup (не тарируется): вставить N новых LandmarkGroup-записей
        //  routine (тарируется):  параллельно удалить все N записей
        // ═════════════════════════════════════════════════════════════════════

        let del_ctr = Arc::new(AtomicU64::new(10_000_000u64));

        {
            let marci_rt2 = marci_rt.clone();
            let conns     = marci_conns.clone();
            let ctr       = del_ctr.clone();

            group.bench_function(
                format!("TO-BE / LandmarkGroup DELETE ×{n} (MarciDB TCP/MDWP → CanopyDB)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            // Setup (не тарируется): вставка N записей
                            let base = ctr.fetch_add(n as u64, Ordering::Relaxed);
                            let ids: Vec<String> = marci_rt2.block_on(async {
                                let mut v = Vec::with_capacity(n);
                                for (j, conn) in conns.iter().enumerate() {
                                    let idx   = base + j as u64;
                                    let frame = mdwp_insert(
                                        "LandmarkGroup",
                                        &serde_json::to_vec(&json!({
                                            "name":        format!("DelTarget marci {}", idx),
                                            "description": "del",
                                        })).unwrap(),
                                    );
                                    let resp = conn.lock().await.roundtrip(frame).await;
                                    let s    = String::from_utf8(resp).unwrap();
                                    let val: Value = serde_json::from_str(&s)
                                        .unwrap_or(Value::String(s));
                                    v.push(marci_value_to_id_url(&val));
                                }
                                v
                            });

                            // Routine (тарируется): N параллельных DELETE
                            let elapsed = marci_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for (j, id_url) in ids.into_iter().enumerate() {
                                    let conn = conns[j].clone();
                                    hs.push(tokio::spawn(async move {
                                        let frame = mdwp_delete("LandmarkGroup", &id_url);
                                        conn.lock().await.roundtrip(frame).await;
                                    }));
                                }
                                for h in hs { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    })
                },
            );
        }

        group.finish();
    }
}

criterion_group!(benches, bench_concurrent_marci);
criterion_main!(benches);