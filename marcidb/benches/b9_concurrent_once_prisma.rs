include!("_bench_common.rs");

fn main() {
    // Размер таблицы: достаточно большой, чтобы запрос был нетривиальным,
    // но не настолько большой, чтобы засев занимал много времени.
    const SEED_SIZE: usize = 10000;

    const LEVELS: &[usize] = &[1, 5, 10, 20, 30, 50];

    for &n in LEVELS {
        eprintln!("\n[bench] b9_concurrent_once_prisma: уровень конкурентности n={n}");

        restart_postgres();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n.max(2))
            .enable_all()
            .build()
            .unwrap();
        let pg_client: Arc<PrismaClient> = Arc::new(
            rt.block_on(PrismaClient::_builder().build())
                .expect("b9_concurrent_once_prisma: не удалось подключиться к PostgreSQL"),
        );

        // Сбрасываем и создаём SEED_SIZE записей Place.
        // 1 запись даёт моментальный результат и не отражает реальную нагрузку;
        // 1000 записей обеспечивают измеримое время сканирования таблицы.
        rt.block_on(async {
            pg_client.place().delete_many(vec![]).exec().await.unwrap();

            for i in 0..SEED_SIZE {
                pg_client
                    .place()
                    .create(
                        format!("Seed_{}", i),
                        "seed".to_string(),
                        format!("{} St", i),
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
            }
        });

        eprintln!("[bench] n={n}: засеяно {SEED_SIZE} записей Place.");

        let monitor = ResourceMonitor::with_warm_up(ServerKind::Postgres, Duration::ZERO);

        let elapsed = monitor.measure_fn(
            &format!("Place READ ×{n} concurrent (Prisma find_many → PostgreSQL, {SEED_SIZE} records)"),
            || {
                let t = Instant::now();
                rt.block_on(async {
                    let mut hs = Vec::with_capacity(n);
                    for _ in 0..n {
                        let c = pg_client.clone();
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
                });
                t.elapsed()
            },
        );

        eprintln!("[timing] n={n} READ ({SEED_SIZE} records): {elapsed:.2?}");
    }
}