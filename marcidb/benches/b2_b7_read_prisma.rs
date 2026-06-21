include!("_bench_common.rs");

fn bench_point_lookup_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
    s:  &Scenario,
) {
    let pg_place_id = pg.place_ids[s.places / 2];
    let pg_lm_id    = pg.landmark_ids[s.landmarks / 2];

    let mut group = c.benchmark_group("read/B2_point_lookup");

    group.bench_function("AS-IS / Place by PK (Prisma find_unique → PostgreSQL)", |b| {
        b.iter(|| pg.engine.find_first_place(pg_place_id))
    });

    group.bench_function("AS-IS / Landmark by PK (Prisma find_unique → PostgreSQL)", |b| {
        b.iter(|| pg.engine.find_first_landmark(pg_lm_id))
    });

    group.finish();
}

// ── B3. Поиск по индексированному полю ───────────────────────────────────────

fn bench_indexed_field_search_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
    s:  &Scenario,
) {

    let k = Scenario::scale();
    let idx_threshold = (s.landmarks * 99 / 100) as i64;


    let mut group = c.benchmark_group("read/B3_indexed_field_search");

    group.bench_function("AS-IS / Landmark indexOnLine (Prisma @@index → PostgreSQL)", |b| {
        b.iter(|| pg.engine.find_many_landmark(&json!({
            "id": true, "$where": {"indexOnLine": {"$gt": idx_threshold}}
        })))
    });

    group.finish();
}

// ── B4. Поиск по НЕиндексированному полю ─────────────────────────────────────

fn bench_non_indexed_field_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
) {

    let mut group = c.benchmark_group("read/B4_non_indexed_field");

    group.bench_function("AS-IS / Place type=museum", |b| {
        b.iter(|| pg.engine.find_many_place(&json!({
            "id": true, "$where": {"type": "museum"}
        })))
    });

    group.bench_function("AS-IS / Place openingHours", |b| {
        b.iter(|| pg.engine.find_many_place(&json!({
            "id": true, "$where": {"openingHours": "10:00-22:00"}
    }   )))
    });

    group.bench_function("AS-IS / Landmark name LIKE (full scan)", |b| {
        b.iter(|| pg.engine.find_many_landmark(&json!({
            "id": true, "$where": {"name": {"$includes": "historical"}}
        })))
    });

    group.finish();
}

// ── B5. Join / связи ──────────────────────────────────────────────────────────

fn bench_joins_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
) {
    let pg_group_id = pg.group_ids[0];
    let pg_lm_id    = pg.landmark_ids[0];
    let pg_user_id  = pg.user_ids[0];

    let mut group = c.benchmark_group("read/B5_joins");

    group.bench_function(
        "AS-IS / AppUser->Favourites (Prisma PK + @@index userId + LEFT JOIN Place/Landmark)",
        |b| {
            b.iter(|| pg.engine.find_user_with_favourites(pg_user_id))
        },
    );

    group.bench_function("AS-IS / Landmark.photoIds (Prisma findUnique, Int[] возврат)", |b| {
        b.iter(|| pg.engine.find_first_landmark_photo_ids(pg_lm_id))
    });

    group.finish();
}

// ── B6. Поиск по events ───────────────────────────────────────────────────────

fn bench_events_search_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
) {
    let pg_lm_id = pg.landmark_ids[0];

    let mut group = c.benchmark_group("read/B6_events_search");

    group.bench_function("AS-IS / AppTour events visit_landmark (JSONB @>, no GIN) [P3]", |b| {
        b.iter(|| pg.engine.find_many_app_tour(&json!({
            "id": true,
            "$where": { "events": { "$some": { "info": "visit_landmark", "landmarkId": { "id": pg_lm_id } } } }
        })))
    });

    group.finish();
}

// ── B7. Обновление ────────────────────────────────────────────────────────────

fn bench_update_prisma(
    c:  &mut Criterion,
    pg: &PrismaEngineFixture,
) {
    let pg_place_id = pg.place_ids[0];
    let pg_lm_id    = pg.landmark_ids[0];

    let mut group = c.benchmark_group("read/B7_update");

    group.bench_function("AS-IS / Place description UPDATE (Prisma → PostgreSQL)", |b| {
        b.iter(|| pg.engine.update_place(pg_place_id, json!({"description": "Updated"})))
    });

    group.bench_function("AS-IS / Landmark description UPDATE (Prisma → PostgreSQL)", |b| {
        b.iter(|| pg.engine.update_landmark(pg_lm_id, json!({"description": "Updated"})))
    });

    group.finish();
}

// ── B2–B7 Prisma. Точка входа ─────────────────────────────────────────────────

fn bench_read_prisma(c: &mut Criterion) {
    let s = Scenario::medium();
    eprintln!(
        "\n[bench] b2_b7_read_prisma | BENCH_SCALE={} | files={} groups={} landmarks={} places={} users={} tours={}",
        Scenario::scale(), s.files, s.groups, s.landmarks, s.places, s.users, s.tours
    );

    let pg = PrismaEngineFixture::new(&s);

    bench_point_lookup_prisma(c, &pg, &s);
    bench_indexed_field_search_prisma(c, &pg, &s);
    bench_non_indexed_field_prisma(c, &pg);
    bench_joins_prisma(c, &pg);
    bench_events_search_prisma(c, &pg);
    bench_update_prisma(c, &pg);

}

criterion_group!(benches, bench_read_prisma);
criterion_main!(benches);