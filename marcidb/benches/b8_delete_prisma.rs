include!("_bench_common.rs");

fn bench_delete_prisma(c: &mut Criterion) {
    let n = Scenario::scale();
    eprintln!("\n[bench] b8_delete_prisma: Prisma → PostgreSQL ({n} children per deleted record)");

    let pg = PrismaEngineClient::new();
    pg.reset_data();

    // Эталонный Place — нужен как цель Favourite в каскадном тесте.
    // Создаём один раз вне iter_batched: setup батча только создаёт
    // AppUser + Favourite, не затрагивая этот Place.
    let pg_ref_place_id = pg.insert_place(json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));


    let mut group = c.benchmark_group("B8_delete");

    // ── LandmarkGroup DELETE + onDelete:SetNull ───────────────────────────────

    group.bench_function(
        format!("AS-IS / LandmarkGroup DELETE + onDelete:SetNull → Landmark ×{n} (Prisma)"),
        |b| {
            b.iter_batched(
                // setup: вставить группу с n Landmark-ами — время НЕ измеряется
                || {
                    let gid = pg.insert_landmark_group(json!({
                        "name": "DelGroup", "description": "desc"
                    }));
                    for j in 0..n {
                        pg.insert_landmark(json!({
                            "name":        format!("Lm {}", j),
                            "description": "desc",
                            "location":    "55.7,37.6",
                            "indexOnLine": j as u64,
                            "groupId":     gid
                        }), vec![]);
                    }
                    gid
                },
                // routine: только удаление — это и измеряется
                |gid| pg.delete_landmark_group(gid),
                criterion::BatchSize::SmallInput,
            )
        },
    );

    // ── AppUser DELETE + onDelete:Cascade → Favourite ─────────────────────────
    //
    // Схема: Favourite.userId → AppUser @onDelete(Cascade)
    // Удаление AppUser должно каскадно удалить все связанные Favourite.
    // В setup создаём пользователя с n Favourite (тип place → pg_ref_place_id).

    group.bench_function(
        format!("AS-IS / AppUser DELETE + onDelete:Cascade → Favourite ×{n} (Prisma)"),
        |b| {
            b.iter_batched(
                // setup: AppUser + n Favourite — время НЕ измеряется
                || {
                    let uid = pg.insert_app_user(json!({
                        "name":           "CascadeUser",
                        "authentication": "vkId",
                        "accessToken":    "tok_cascade",
                        "address":        "addr_cascade"
                    }));
                    for _ in 0..n {
                        pg.insert_favourite(json!({
                            "userId":  uid,
                            "type":    "place",
                            "placeId": pg_ref_place_id
                        }));
                    }
                    uid
                },
                // routine: удаление пользователя — Favourite удаляются каскадно
                |uid| pg.delete_app_user(uid),
                criterion::BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

criterion_group!(benches, bench_delete_prisma);
criterion_main!(benches);