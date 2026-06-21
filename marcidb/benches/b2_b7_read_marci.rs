include!("_bench_common.rs");

// ── B2. Точечный поиск по первичному ключу ────────────────────────────────────

fn bench_point_lookup_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
    s:     &Scenario,
) {
    let marci_place_id = marci.place_ids[s.places / 2].clone();
    let marci_lm_id    = marci.landmark_ids[s.landmarks / 2].clone();

    let mut group = c.benchmark_group("read/B2_point_lookup");

    group.bench_function("TO-BE / Place by PK (MarciDB TCP/MDWP → CanopyDB)", |b| {
        b.iter(|| marci.engine.find_first("Place", &json!({
            "id": true, "$where": marci_place_id
        })))
    });

    group.bench_function("TO-BE / Landmark by PK (MarciDB TCP/MDWP → CanopyDB)", |b| {
        b.iter(|| marci.engine.find_first("Landmark", &json!({
            "id": true, "$where": marci_lm_id
        })))
    });

    group.finish();
}

// ── B3. Поиск по индексированному полю ───────────────────────────────────────

fn bench_indexed_field_search_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
    s:  &Scenario,
) {

    let k = Scenario::scale();
    let idx_threshold = (s.landmarks * 99 / 100) as i64;

    let mut group = c.benchmark_group("read/B3_indexed_field_search");

    group.bench_function("TO-BE / Landmark indexOnLine>20 (MarciDB @index via TCP/MDWP)", |b| {
        b.iter(|| marci.engine.find_many("Landmark", &json!({
            "id": true, "$where": {"indexOnLine": {"$gt": idx_threshold}}
        })))
    });

    group.finish();
}

fn bench_non_indexed_field_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
) {

    let mut group = c.benchmark_group("read/B4_non_indexed_field");

    group.bench_function("TO-BE / Place type=museum  [P1]", |b| {
        b.iter(|| marci.engine.find_many("Place", &json!({
            "id": true, "$where": {"type": "museum"}
        })))
    });

    group.bench_function("TO-BE / Place openingHours (enum variant field, full scan via TCP/MDWP)", |b| {
        b.iter(|| marci.engine.find_many("Place", &json!({
            "id": true, "$where": {"openingHours": "10:00-22:00"}
        })))
    });

    group.bench_function("TO-BE / Landmark name $includes (full scan via TCP/MDWP)", |b| {
        b.iter(|| marci.engine.find_many("Landmark", &json!({
            "id": true, "$where": {"name": {"$includes": "historical"}}
        })))
    });

    group.finish();
}

// ── B5. Join / связи ──────────────────────────────────────────────────────────

fn bench_joins_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
) {
    let marci_group_id = marci.group_ids[0].clone();
    let marci_lm_id    = marci.landmark_ids[0].clone();
    let marci_user_id  = marci.user_ids[0].clone();

    let mut group = c.benchmark_group("read/B5_joins");

    group.bench_function("TO-BE / AppUser->Favourites (MarciDB @bind prefix scan via TCP/MDWP)", |b| {
        b.iter(|| marci.engine.find_first("AppUser", &json!({
            "name": true,
            "favourites": {
                "id":        true,
                "object":    true,
                "createdAt": true,
                "placeId": {"id": true, "name": true},
                "landmarkId": {"id": true, "name": true}
            },
            "$where": marci_user_id
        })))
    });

    group.bench_function("TO-BE / Landmark.photoIds (MarciDB индексное дерево File[])", |b| {
        b.iter(|| marci.engine.find_first("Landmark", &json!({
            "id": true,
            "photoIds": { "id": true },
            "$where": marci_lm_id
        })))
    });

    group.finish();
}

// ── B6. Поиск по events ───────────────────────────────────────────────────────

fn bench_events_search_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
) {
    let marci_lm_id = marci.landmark_ids[0].get("id").unwrap().as_u64().unwrap();

    let mut group = c.benchmark_group("read/B6_events_search");

    group.bench_function("TO-BE / AppTour events visit_landmark (typed Events[] via TCP/MDWP) [P3]", |b| {
        b.iter(|| marci.engine.find_many("AppTour", &json!({
            "id": true,
            "$where": { "events": { "$some": { "info": "visit_landmark", "landmarkId": { "id": marci_lm_id } } } }
        })))
    });

    group.finish();
}

// ── B7. Обновление ────────────────────────────────────────────────────────────

fn bench_update_marci(
    c:     &mut Criterion,
    marci: &MarciEngineFixture,
) {
    let marci_place_id = marci.place_ids[0].clone();
    let marci_lm_id    = marci.landmark_ids[0].clone();

    let mut group = c.benchmark_group("read/B7_update");

    group.bench_function("TO-BE / Place description UPDATE (MarciDB TCP/MDWP → CanopyDB)", |b| {
        b.iter(|| marci.engine.update("Place", &marci_place_id, &json!({"description": "Updated"})))
    });

    group.bench_function("TO-BE / Landmark description UPDATE (MarciDB TCP/MDWP → CanopyDB)", |b| {
        b.iter(|| marci.engine.update("Landmark", &marci_lm_id, &json!({"description": "Updated"})))
    });

    group.finish();
}

// ── B2–B7 MarciDB. Точка входа ────────────────────────────────────────────────

fn bench_read_marci(c: &mut Criterion) {
    let s = Scenario::medium();
    eprintln!(
        "\n[bench] b2_b7_read_marci | BENCH_SCALE={} | files={} groups={} landmarks={} places={} users={} tours={}",
        Scenario::scale(), s.files, s.groups, s.landmarks, s.places, s.users, s.tours
    );

    let marci = MarciEngineFixture::new(&s);

    bench_point_lookup_marci(c, &marci, &s);
    bench_indexed_field_search_marci(c, &marci, &s);
    bench_non_indexed_field_marci(c, &marci);
    bench_joins_marci(c, &marci);
    bench_events_search_marci(c, &marci);
    bench_update_marci(c, &marci);
}

criterion_group!(benches, bench_read_marci);
criterion_main!(benches);