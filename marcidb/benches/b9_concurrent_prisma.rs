include!("_bench_common.rs");

fn bench_concurrent_prisma(c: &mut Criterion) {
    const LEVELS:    &[usize] = &[1, 5, 10, 20, 30, 50];
    // Фиксированный размер таблицы для READ / UPDATE.
    // Не зависит от уровня конкурентности — условия одинаковы для всех n.
    const SEED_SIZE: usize = 1000;

    for &n in LEVELS {
        eprintln!("\n[bench] b9_concurrent_prisma: уровень конкурентности n={n}");

        // ── Перезапуск PostgreSQL не нужен между операциями ───────────────────
        //
        //  Цель бенчмарка — сравнение двух систем в одинаковых условиях.
        //  Тёплый кэш у обеих сторон честен и уменьшает шум от I/O.
        //  Полный сброс выполняется только программной очисткой данных ниже,
        //  что обеспечивает детерминированный размер таблицы между операциями.

        let mut group = c.benchmark_group(format!("B9_concurrent_prisma/concurrency={n}"));
        group.throughput(Throughput::Elements(n as u64));

        // ── Prisma: многопоточный Runtime + shared Arc<PrismaClient> ─────────
        //
        //  PrismaClient внутри держит пул соединений к PostgreSQL;
        //  N tokio-задач разделяют один клиент — честное отражение реального
        //  сервисного кода, где клиент — singleton.

        let pg_rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(n.max(2))
                .enable_all()
                .build()
                .unwrap(),
        );
        let pg_client: Arc<PrismaClient> = Arc::new(
            pg_rt
                .block_on(PrismaClient::_builder().build())
                .expect("bench_concurrent_prisma: не удалось подключиться к PostgreSQL"),
        );

        // Очищаем PostgreSQL перед каждым уровнем конкурентности.
        pg_rt.block_on(async {
            pg_client.favourite().delete_many(vec![]).exec().await.unwrap();
            pg_client.app_tour().delete_many(vec![]).exec().await.unwrap();
            pg_client.landmark().delete_many(vec![]).exec().await.unwrap();
            pg_client.landmark_group().delete_many(vec![]).exec().await.unwrap();
            pg_client.place().delete_many(vec![]).exec().await.unwrap();
            pg_client.app_user().delete_many(vec![]).exec().await.unwrap();
            pg_client.file().delete_many(vec![]).exec().await.unwrap();
        });

        // ═════════════════════════════════════════════════════════════════════
        //  INSERT: N параллельных вставок Place
        //
        //  Предварительный засев не нужен: бенчмарк сам создаёт записи.
        //  После окончания INSERT-бенчмарка таблица очищается и засевается
        //  ровно SEED_SIZE записями для READ и UPDATE (см. ниже).
        // ═════════════════════════════════════════════════════════════════════

        let ins_ctr = Arc::new(AtomicU64::new(0u64));

        {
            let pg_rt2 = pg_rt.clone();
            let pg_cl  = pg_client.clone();
            let ctr    = ins_ctr.clone();

            group.bench_function(
                format!("AS-IS / Place INSERT ×{n} (Prisma → PostgreSQL)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let base    = ctr.fetch_add(n as u64, Ordering::Relaxed);
                            let elapsed = pg_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for j in 0..n {
                                    let c   = pg_cl.clone();
                                    let idx = base + j as u64;
                                    hs.push(tokio::spawn(async move {
                                        prisma_insert_place(&c, json!({
                                            "name":        format!("ConcIns pg {}", idx),
                                            "description": "desc",
                                            "address":     "1 St",
                                            "type":        "food",
                                            "averageBill": 800i64
                                        })).await;
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
        //  Criterion выполняет десятки итераций во время warm-up и измерений,
        //  каждая вставляет N записей. После окончания INSERT-бенчмарка
        //  в таблице может быть тысячи строк, что исказило бы READ и UPDATE.
        //  Удаляем всё и создаём ровно SEED_SIZE записей: результаты READ / UPDATE
        //  не зависят от случайного числа итераций Criterion.

        eprintln!(
            "[bench] n={n}: очистка после INSERT, засев {SEED_SIZE} записей для READ/UPDATE..."
        );

        let pg_seed_ids: Arc<Vec<i32>> = Arc::new(pg_rt.block_on(async {
            pg_client.place().delete_many(vec![]).exec().await.unwrap();

            let mut ids = Vec::with_capacity(SEED_SIZE);
            for i in 0..SEED_SIZE {
                let p = pg_client
                    .place()
                    .create(
                        format!("ReadSeed_n{}_r{}", n, i),
                        "seed".to_string(),
                        "seed addr".to_string(),
                        "food".to_string(),
                        vec![
                            prisma::place::average_bill::set(Some(500)),
                            prisma::place::tags::set(vec![]),
                            prisma::place::photo_ids::set(vec![]),
                            prisma::place::simillar_places::set(vec![]),
                        ],
                    )
                    .exec()
                    .await
                    .unwrap();
                ids.push(p.id);
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
            let pg_rt2 = pg_rt.clone();
            let pg_cl  = pg_client.clone();

            group.bench_function(
                format!("AS-IS / Place READ ×{n} (Prisma find_many → PostgreSQL)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = pg_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for _ in 0..n {
                                    let c = pg_cl.clone();
                                    hs.push(tokio::spawn(async move {
                                        c.place()
                                            .find_many(vec![])
                                            .select(prisma::place::select!({ id }))
                                            .exec()
                                            .await
                                            .unwrap();
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
        //  pg_seed_ids содержит SEED_SIZE ID-ов; используем [j % SEED_SIZE],
        //  чтобы корректно работать при любом n ≤ SEED_SIZE.
        // ═════════════════════════════════════════════════════════════════════

        {
            let pg_rt2 = pg_rt.clone();
            let pg_cl  = pg_client.clone();
            let pg_ids = pg_seed_ids.clone();

            group.bench_function(
                format!("AS-IS / Place UPDATE ×{n} (Prisma → PostgreSQL)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = pg_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for j in 0..n {
                                    let c  = pg_cl.clone();
                                    let id = pg_ids[j % SEED_SIZE];
                                    hs.push(tokio::spawn(async move {
                                        prisma_update_place(
                                            &c,
                                            id,
                                            json!({ "description": "ConcUpdated" }),
                                        ).await;
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
            let pg_rt2 = pg_rt.clone();
            let pg_cl  = pg_client.clone();
            let ctr    = del_ctr.clone();

            group.bench_function(
                format!("AS-IS / LandmarkGroup DELETE ×{n} (Prisma → PostgreSQL)"),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            // Setup (не тарируется): вставка N записей
                            let base = ctr.fetch_add(n as u64, Ordering::Relaxed);
                            let ids: Vec<i32> = pg_rt2.block_on(async {
                                let mut v = Vec::with_capacity(n);
                                for j in 0..n {
                                    let idx = base + j as u64;
                                    let g = pg_cl
                                        .landmark_group()
                                        .create(
                                            format!("DelTarget pg {}", idx),
                                            vec![prisma::landmark_group::description::set(
                                                "del".to_string(),
                                            )],
                                        )
                                        .exec()
                                        .await
                                        .unwrap();
                                    v.push(g.id);
                                }
                                v
                            });

                            // Routine (тарируется): N параллельных DELETE
                            let elapsed = pg_rt2.block_on(async {
                                let start = std::time::Instant::now();
                                let mut hs = Vec::with_capacity(n);
                                for id in ids {
                                    let c = pg_cl.clone();
                                    hs.push(tokio::spawn(async move {
                                        c.landmark_group()
                                            .delete(prisma::landmark_group::id::equals(id))
                                            .exec()
                                            .await
                                            .unwrap();
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

criterion_group!(benches, bench_concurrent_prisma);
criterion_main!(benches);