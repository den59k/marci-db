include!("_bench_common.rs");

fn bench_insert_marci(c: &mut Criterion) {
    let n = Scenario::scale();
    eprintln!("\n[bench] b1_insert_marci: MarciDB TCP/MDWP → CanopyDB ({n} records per iteration)");

    let marci = MarciTcpClient::new();

    let marci_ref_place = marci.insert("Place", &json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));

    let mut group = c.benchmark_group("B1_insert_marci");
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
        format!("Place ×{n} sequential (MarciDB TCP/MDWP → CanopyDB)"),
        |b| b.iter(|| {
            for rec in &place_records { marci.insert("Place", rec); }
        }),
    );

    // ── AppUser ──────────────────────────────────────────────────────────────

    let user_ctr = Arc::new(AtomicU64::new(0));
    {
        let ctr = user_ctr.clone();
        group.bench_function(
            format!("AppUser ×{n} sequential (MarciDB TCP/MDWP → CanopyDB)"),
            |b| b.iter(|| {
                let base = ctr.fetch_add(n as u64, Ordering::Relaxed);
                for i in 0..n as u64 {
                    marci.insert("AppUser", &json!({
                        "name":           format!("User {}", base + i),
                        "authentication": "vkId",
                        "accessToken":    format!("tok_{}", base + i),
                        "address":        format!("addr_{}", base + i)
                    }));
                }
            }),
        );
    }

    // ── AppTour + events ─────────────────────────────────────────────────────

    let marci_tour_records: Vec<Value> = (0..n).map(|i| json!({
        "title": format!("Tour {}", i),
        "text":  "text",
        "start": "2026-06-01T10:00:00Z",
        "end":   "2026-06-02T10:00:00Z",
        "events": [{ "order": 1, "info": "eating", "placeId": marci_ref_place, "time": "2026-06-01T12:00:00Z" }]
    })).collect();

    group.bench_function(
        format!("AppTour+events ×{n} sequential (MarciDB typed Events[] via TCP/MDWP)"),
        |b| b.iter(|| {
            for rec in &marci_tour_records { marci.insert("AppTour", rec); }
        }),
    );

    group.finish();
}

criterion_group!(benches, bench_insert_marci);
criterion_main!(benches);