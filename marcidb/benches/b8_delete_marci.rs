include!("_bench_common.rs");

fn bench_delete_marci(c: &mut Criterion) {
    let n = Scenario::scale();
    eprintln!("\n[bench] b8_delete_marci: MarciDB TCP/MDWP → CanopyDB ({n} children per deleted record)");

    let marci = MarciTcpClient::new();

    let marci_ref_place = marci.insert("Place", &json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));

    let mut group = c.benchmark_group("B8_delete");

    // ── LandmarkGroup DELETE + SetNull @bind ──────────────────────────────────

    group.bench_function(
        format!("TO-BE / LandmarkGroup DELETE + SetNull @bind → Landmark ×{n} (MarciDB TCP/MDWP)"),
        |b| {
            b.iter_batched(
                || {
                    let gid = marci.insert("LandmarkGroup", &json!({
                        "name": "DelGroup", "description": "desc"
                    }));
                    for j in 0..n {
                        marci.insert("Landmark", &json!({
                            "name":        format!("Lm {}", j),
                            "description": "desc",
                            "location":    "55.7,37.6",
                            "indexOnLine": j as u64,
                            "groupId":     gid
                        }));
                    }
                    gid
                },
                |gid| marci.delete("LandmarkGroup", &gid),
                criterion::BatchSize::SmallInput,
            )
        },
    );

    // ── AppUser DELETE + @onDelete(CASCADE) → Favourite ───────────────────────

    group.bench_function(
        format!("TO-BE / AppUser DELETE + @onDelete(CASCADE) → Favourite ×{n} (MarciDB TCP/MDWP)"),
        |b| {
            b.iter_batched(
                || {
                    let uid = marci.insert("AppUser", &json!({
                        "name":           "CascadeUser",
                        "authentication": "vkId",
                        "accessToken":    "tok_cascade",
                        "address":        "addr_cascade"
                    }));
                    for _ in 0..n {
                        marci.insert("Favourite", &json!({
                            "userId":  uid,
                            "object":  "place",
                            "placeId": marci_ref_place
                        }));
                    }
                    uid
                },
                |uid| marci.delete("AppUser", &uid),
                criterion::BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

criterion_group!(benches, bench_delete_marci);
criterion_main!(benches);