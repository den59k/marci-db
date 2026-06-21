include!("_bench_common.rs");

fn bench_insert_prisma(c: &mut Criterion) {
    let n = Scenario::scale();
    eprintln!("\n[bench] b1_insert_prisma: Prisma → PostgreSQL ({n} records per iteration)");

    let pg = PrismaEngineClient::new();
    pg.reset_data();

    let pg_ref_place_id = pg.insert_place(json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));

    let mut group = c.benchmark_group("B1_insert_prisma");
    group.throughput(Throughput::Elements(n as u64));

    // ── Place ────────────────────────────────────────────────────────────────

    let place_records: Vec<Value> = (0..n).map(|i| json!({
        "name":              format!("Bench Food {}", i),
        "description":       "desc",
        "address":           format!("{} St", i),
        "type":              "food",
        "averageBill":       800i64,
        "deliveryAvailable": true,
        "openingHours":      "10:00-22:00"
    })).collect();

    group.bench_function(
        format!("Place ×{n} create_many (Prisma → PostgreSQL)"),
        |b| b.iter(|| pg.create_many_place(&place_records)),
    );

    // ── AppUser ──────────────────────────────────────────────────────────────

    let user_ctr = Arc::new(AtomicU64::new(0));
    {
        let ctr = user_ctr.clone();
        group.bench_function(
            format!("AppUser ×{n} create_many (Prisma → PostgreSQL)"),
            |b| b.iter(|| {
                let base = ctr.fetch_add(n as u64, Ordering::Relaxed);
                let records: Vec<Value> = (0..n as u64).map(|i| json!({
                    "name":           format!("User {}", base + i),
                    "authentication": "vkId",
                    "accessToken":    format!("tok_{}", base + i),
                    "address":        format!("addr_{}", base + i)
                })).collect();
                pg.create_many_app_user(&records)
            }),
        );
    }

    // ── AppTour + events ─────────────────────────────────────────────────────

    let pg_tour_records: Vec<Value> = (0..n).map(|i| json!({
        "title": format!("Tour {}", i),
        "text":  "text",
        "start": "2026-06-01T10:00:00Z",
        "end":   "2026-06-02T10:00:00Z",
        "events": [{ "order": 1, "type": "eating", "placeId": pg_ref_place_id, "time": "2026-06-01T12:00:00Z" }]
    })).collect();

    group.bench_function(
        format!("AppTour+events ×{n} create_many (Prisma Json → PostgreSQL JSONB)"),
        |b| b.iter(|| pg.create_many_app_tour(&pg_tour_records)),
    );

    group.finish();
}

criterion_group!(benches, bench_insert_prisma);
criterion_main!(benches);