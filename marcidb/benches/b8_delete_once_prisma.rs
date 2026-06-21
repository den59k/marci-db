include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b8_delete_once_prisma  BENCH_SCALE={n}  (LandmarkGroup + {n} Landmark → SetNull)");

    restart_postgres();
    let pg = PrismaEngineClient::new();
    pg.reset_data();

    // ── 1. SetNull: LandmarkGroup → Landmark ─────────────────────────────────

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

    let monitor = ResourceMonitor::with_warm_up(ServerKind::Postgres, Duration::ZERO);

    let elapsed = monitor.measure_fn(
        &format!("LandmarkGroup DELETE + onDelete:SetNull → Landmark ×{n} (Prisma → PostgreSQL)"),
        || {
            let t = Instant::now();
            pg.delete_landmark_group(gid);
            t.elapsed()
        },
    );

    eprintln!("[timing] LandmarkGroup DELETE ({n} children): {elapsed:.2?}");

    // ── 2. CASCADE: AppUser → Favourite ──────────────────────────────────────

    let ref_place_id = pg.insert_place(json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));

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
            "placeId": ref_place_id
        }));
    }

    let elapsed = monitor.measure_fn(
        &format!("AppUser DELETE + onDelete:Cascade → Favourite ×{n} (Prisma → PostgreSQL)"),
        || {
            let t = Instant::now();
            pg.delete_app_user(uid);
            t.elapsed()
        },
    );

    eprintln!("[timing] AppUser DELETE ({n} children): {elapsed:.2?}");
}