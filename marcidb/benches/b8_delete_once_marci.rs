include!("_bench_common.rs");

fn main() {
    let n = Scenario::scale();
    eprintln!("\n[bench] b8_delete_once_marci  BENCH_SCALE={n}  (LandmarkGroup + {n} Landmark → SetNull)");

    let marci = MarciTcpClient::new();

    // ── 1. SetNull: LandmarkGroup → Landmark ─────────────────────────────────

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

    let monitor = ResourceMonitor::with_warm_up(ServerKind::MarciDB, Duration::ZERO);

    let elapsed = monitor.measure_fn(
        &format!("LandmarkGroup DELETE + SetNull @bind → Landmark ×{n} (MarciDB TCP/MDWP → CanopyDB)"),
        || {
            let t = Instant::now();
            marci.delete("LandmarkGroup", &gid);
            t.elapsed()
        },
    );

    eprintln!("[timing] LandmarkGroup DELETE ({n} children): {elapsed:.2?}");

    // ── 2. CASCADE: AppUser → Favourite ──────────────────────────────────────

    let ref_place = marci.insert("Place", &json!({
        "name": "Ref Place", "description": "ref", "address": "addr",
        "type": "food", "averageBill": 500, "deliveryAvailable": true, "openingHours": "10-22"
    }));

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
            "placeId": ref_place
        }));
    }

    let elapsed = monitor.measure_fn(
        &format!("AppUser DELETE + @onDelete(CASCADE) → Favourite ×{n} (MarciDB TCP/MDWP → CanopyDB)"),
        || {
            let t = Instant::now();
            marci.delete("AppUser", &uid);
            t.elapsed()
        },
    );

    eprintln!("[timing] AppUser DELETE ({n} children): {elapsed:.2?}");
}