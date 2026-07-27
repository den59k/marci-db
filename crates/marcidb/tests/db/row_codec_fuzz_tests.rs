//! Model-based randomized test over the row codec.
//!
//! Every write path edits a row in place: a value that grows or shrinks moves the payload, and every
//! offset after it has to move with it. Get that wrong and the *neighbouring* fields decode as garbage —
//! silent, persisted corruption that a hand-written test only notices if it happens to put the mutated
//! field before another one. That is exactly how the `SetNull` offset bug survived the suite: every
//! delete test had its foreign key last in the model.
//!
//! So this test does not fix a shape. It generates a schema (random field types, random order, random
//! nullability, the relation at a random position), runs a random sequence of writes against it, and
//! keeps a shadow copy of what every row should read back as — compared after EVERY operation, so a
//! failure names the exact op that broke it. Deterministic: a failing seed reproduces exactly.

use std::collections::BTreeMap;

use marcidb::{DeleteError, MarciDB, try_parse_schema};
use marcidb_schema::{diff, reconcile};
use serde_json::{Map, Value, json};
use tempfile::TempDir;

use crate::db::{get_data, insert_data, try_delete, try_update};

// ─────────────────────────────── deterministic PRNG ───────────────────────────────

/// xorshift64* — deterministic across platforms and Rust versions, so a reported seed always
/// reproduces the same run (`rand` is not a dependency and a wandering algorithm would defeat the point).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    /// `true` with probability `pct`%
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

// ─────────────────────────────── generated schema ───────────────────────────────

#[derive(Clone, Copy, PartialEq, Debug)]
enum Ty {
    Str,
    Int,
    UInt,
    Bool,
    Double,
    StrList,
    Json,
    HexBytes,
    DateTime,
}

impl Ty {
    fn decl(self) -> &'static str {
        match self {
            Ty::Str => "String",
            Ty::Int => "Int",
            Ty::UInt => "UInt",
            Ty::Bool => "Bool",
            Ty::Double => "Double",
            Ty::StrList => "String[]",
            Ty::Json => "Json",
            Ty::HexBytes => "Byte[6]",
            Ty::DateTime => "DateTime",
        }
    }
    /// A list is never null — an absent list reads back as `[]` (absence = empty), so it has no
    /// null state to model and `?` would only make the oracle lie.
    fn nullable_allowed(self) -> bool {
        self != Ty::StrList
    }
    fn numeric(self) -> bool {
        matches!(self, Ty::Int | Ty::UInt | Ty::Double)
    }
}

const TYPES: [Ty; 9] = [
    Ty::Str, Ty::Int, Ty::UInt, Ty::Bool, Ty::Double,
    Ty::StrList, Ty::Json, Ty::HexBytes, Ty::DateTime,
];

struct FieldDef {
    name: String,
    ty: Ty,
    nullable: bool,
}

struct Shape {
    fields: Vec<FieldDef>,
    /// Whether the generated relation is optional (`SetNull` on target delete) or required (`Restrict`)
    rel_optional: bool,
    /// Position of the relation among the fields — the whole point is that it is often NOT last
    rel_at: usize,
    /// Names are never reused, so a field added mid-run cannot collide with an existing one
    next_field_id: usize,
}

impl Shape {
    fn generate(rng: &mut Rng) -> Shape {
        let count = 3 + rng.below(6); // 3..8 scalar fields
        let fields = (0..count)
            .map(|i| {
                let ty = TYPES[rng.below(TYPES.len())];
                FieldDef {
                    name: format!("f{}", i),
                    ty,
                    nullable: ty.nullable_allowed() && rng.chance(60),
                }
            })
            .collect::<Vec<_>>();
        Shape {
            rel_at: rng.below(fields.len() + 1),
            rel_optional: rng.chance(70),
            next_field_id: fields.len(),
            fields,
        }
    }

    fn schema_text(&self) -> String {
        let mut lines = vec!["model Target {".to_string(), "    label String".to_string(), "}".to_string(), String::new(), "model Main {".to_string()];
        let rel = format!("    rel Target{}", if self.rel_optional { "?" } else { "" });
        for (i, f) in self.fields.iter().enumerate() {
            if i == self.rel_at {
                lines.push(rel.clone());
            }
            lines.push(format!("    {} {}{}", f.name, f.ty.decl(), if f.nullable { "?" } else { "" }));
        }
        if self.rel_at == self.fields.len() {
            lines.push(rel);
        }
        lines.push("}".to_string());
        lines.join("\n")
    }

    /// `{ id: true, f0: true, …, rel: { id: true } }` — every field, so a mismatch anywhere shows up
    fn selection(&self) -> Value {
        let mut sel = Map::new();
        sel.insert("id".into(), json!(true));
        for f in self.fields.iter() {
            sel.insert(f.name.clone(), json!(true));
        }
        sel.insert("rel".into(), json!({ "id": true }));
        Value::Object(sel)
    }
}

// ─────────────────────────────── value generation ───────────────────────────────

/// Lengths vary widely on purpose: a same-length overwrite takes the in-place branch, a different
/// length takes the splice-and-shift branch. Only the second one can corrupt neighbours.
fn gen_value(rng: &mut Rng, ty: Ty) -> Value {
    match ty {
        Ty::Str => {
            let len = rng.below(24);
            let s: String = (0..len).map(|_| (b'a' + rng.below(26) as u8) as char).collect();
            json!(s)
        }
        Ty::Int => json!(rng.next() as i32 as i64 / 3),
        Ty::UInt => json!(rng.next() % 1_000_000),
        Ty::Bool => json!(rng.chance(50)),
        Ty::Double => json!((rng.next() % 100_000) as f64 / 8.0),
        Ty::StrList => {
            let n = rng.below(4);
            let items: Vec<Value> = (0..n).map(|_| gen_value(rng, Ty::Str)).collect();
            Value::Array(items)
        }
        Ty::Json => {
            if rng.chance(50) {
                json!({ "k": rng.next() % 500, "s": gen_value(rng, Ty::Str) })
            } else {
                json!([rng.next() % 100, gen_value(rng, Ty::Str)])
            }
        }
        Ty::HexBytes => {
            let bytes: String = (0..12).map(|_| char::from_digit(rng.below(16) as u32, 16).unwrap()).collect();
            json!(bytes)
        }
        Ty::DateTime => json!(1_600_000_000_000i64 + (rng.next() % 100_000_000) as i64),
    }
}

/// What the engine reads back for a value that was written. Everything round-trips as written except
/// `Byte[N]`, whose canonical read form is a byte array (the write form is a hex string).
fn read_form(ty: Ty, written: &Value) -> Value {
    match ty {
        Ty::HexBytes => {
            let hex = written.as_str().expect("HexBytes is written as a hex string");
            let bytes: Vec<Value> = (0..hex.len() / 2)
                .map(|i| json!(u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap()))
                .collect();
            Value::Array(bytes)
        }
        _ => written.clone(),
    }
}

/// The value an absent field reads back as: a list is empty, everything else is null.
fn absent_form(ty: Ty) -> Value {
    if ty == Ty::StrList { json!([]) } else { Value::Null }
}

// ─────────────────────────────── mid-run migration ───────────────────────────────

/// Adds a field mid-run through the real migration path (reconcile → diff → `commit_schema`).
///
/// This is the reason the harness bothers with migrations at all: rows written *before* the migration
/// keep the older, SHORTER offset table, and every later write to such a row has to widen it first.
/// Reading and mutating short rows is a separate code path from the current-layout one, and it is
/// where the second half of the `SetNull` bug lived (bounds taken from the schema's `payload_offset`
/// instead of the row's own header).
fn migrate_add_field(db: &mut MarciDB, shape: &mut Shape, rng: &mut Rng, shadow: &mut Shadow) -> String {
    let ty = TYPES[rng.below(TYPES.len())];
    let name = format!("f{}", shape.next_field_id);
    shape.next_field_id += 1;

    // Insert at a random position in the DECLARATION, not necessarily the end: slot assignment must come
    // from `reconcile` carrying history, never from declaration order.
    let at = rng.below(shape.fields.len() + 1);
    if at <= shape.rel_at {
        shape.rel_at += 1;
    }
    shape.fields.insert(at, FieldDef { name: name.clone(), ty, nullable: ty.nullable_allowed() });

    let mut new_schema = try_parse_schema(&shape.schema_text()).expect("generated schema must parse");
    reconcile(&mut new_schema, &db.schema);
    let ops = diff(&db.schema, &new_schema).expect("adding a nullable field must be migratable");
    db.commit_schema(new_schema, &ops).expect("migration must apply");

    // Every existing row predates the field, so it reads back as absent
    for row in shadow.values_mut() {
        row.insert(name.clone(), absent_form(ty));
    }
    format!("migrate: add field {} {} at position {}", name, ty.decl(), at)
}

// ─────────────────────────────── the shadow model ───────────────────────────────

/// Expected state of every live `Main` row, keyed by id. The oracle: whatever the engine returns must
/// equal this, exactly, after every single operation.
type Shadow = BTreeMap<u64, Map<String, Value>>;

fn id_of(handle: &Value) -> u64 {
    handle["id"].as_u64().expect("generated ids are UInt")
}

/// Reads every `Main` row and indexes it by id, so the comparison is order-independent and a
/// difference can be attributed to one row.
fn read_rows(db: &MarciDB, shape: &Shape) -> BTreeMap<u64, Map<String, Value>> {
    let rows = get_data(db, "Main", shape.selection());
    rows.as_array()
        .expect("findMany returns an array")
        .iter()
        .map(|row| {
            let obj = row.as_object().expect("a row is an object").clone();
            (obj["id"].as_u64().expect("id is a number"), obj)
        })
        .collect()
}

fn check(db: &MarciDB, shape: &Shape, shadow: &Shadow, seed: u64, log: &[String]) {
    let actual = read_rows(db, shape);
    if &actual == shadow {
        return;
    }

    // Point at the first row that differs, and at which field — a whole-table dump is unreadable.
    let mut detail = String::new();
    for (id, want) in shadow.iter() {
        match actual.get(id) {
            None => detail.push_str(&format!("row {} is missing\n", id)),
            Some(got) if got != want => {
                for (k, want_v) in want.iter() {
                    let got_v = got.get(k).unwrap_or(&Value::Null);
                    if got_v != want_v {
                        detail.push_str(&format!("row {} field {}: expected {}, got {}\n", id, k, want_v, got_v));
                    }
                }
            }
            _ => {}
        }
    }
    for id in actual.keys() {
        if !shadow.contains_key(id) {
            detail.push_str(&format!("row {} should have been deleted\n", id));
        }
    }

    panic!(
        "row codec diverged from the shadow model (seed {})\n\n{}\nschema:\n{}\n\nlast ops:\n  {}",
        seed, detail, shape.schema_text(),
        log.iter().rev().take(12).rev().cloned().collect::<Vec<_>>().join("\n  "),
    );
}

// ─────────────────────────────── the run ───────────────────────────────

fn run_seed(seed: u64, ops: usize) {
    let mut rng = Rng::new(seed);
    let mut shape = Shape::generate(&mut rng);
    let schema_text = shape.schema_text();

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mut db = MarciDB::new(&schema_text, &path);

    let mut shadow: Shadow = BTreeMap::new();
    let mut log: Vec<String> = vec![];
    // Which target each live row points at, so a target delete can be modelled without re-reading
    let mut points_at: BTreeMap<u64, u64> = BTreeMap::new();
    let mut targets: Vec<Value> = vec![];

    for step in 0..ops {
        // Keep a pool of targets available for the relation to point at
        if targets.len() < 3 {
            targets.push(insert_data(&db, "Target", json!({ "label": format!("t{}", step) })));
        }

        match rng.below(11) {
            // ── insert ──────────────────────────────────────────────────────────────────────
            0..=3 => {
                let mut data = Map::new();
                let mut expected = Map::new();
                for f in shape.fields.iter() {
                    // A nullable field is sometimes omitted entirely: the slot stays 0 and the payload
                    // is shorter, which is a different physical row from "present but null"
                    if f.nullable && rng.chance(30) {
                        expected.insert(f.name.clone(), absent_form(f.ty));
                        continue;
                    }
                    let v = gen_value(&mut rng, f.ty);
                    expected.insert(f.name.clone(), read_form(f.ty, &v));
                    data.insert(f.name.clone(), v);
                }

                // A required relation must always be given; an optional one sometimes
                let target = if !shape.rel_optional || rng.chance(70) {
                    Some(targets[rng.below(targets.len())].clone())
                } else {
                    None
                };
                match &target {
                    Some(t) => {
                        data.insert("rel".into(), t.clone());
                        expected.insert("rel".into(), json!({ "id": id_of(t) }));
                    }
                    None => {
                        expected.insert("rel".into(), Value::Null);
                    }
                }

                let handle = insert_data(&db, "Main", Value::Object(data.clone()));
                let id = id_of(&handle);
                expected.insert("id".into(), json!(id));
                if let Some(t) = &target {
                    points_at.insert(id, id_of(t));
                }
                shadow.insert(id, expected);
                log.push(format!("insert Main#{} {}", id, Value::Object(data)));
            }

            // ── update: overwrite / null out a random subset of fields ──────────────────────
            4..=6 => {
                let Some(&id) = pick(&mut rng, &shadow.keys().copied().collect::<Vec<_>>()) else { continue };
                let mut data = Map::new();
                let expected = shadow.get_mut(&id).unwrap();
                for f in shape.fields.iter() {
                    if !rng.chance(45) {
                        continue;
                    }
                    // Nulling a present value shrinks the row; writing a value can grow or shrink it
                    if f.nullable && rng.chance(30) {
                        data.insert(f.name.clone(), Value::Null);
                        expected.insert(f.name.clone(), Value::Null);
                    } else {
                        let v = gen_value(&mut rng, f.ty);
                        expected.insert(f.name.clone(), read_form(f.ty, &v));
                        data.insert(f.name.clone(), v);
                    }
                }
                if data.is_empty() {
                    continue;
                }
                try_update(&db, "Main", &json!({ "id": id }), Value::Object(data.clone()))
                    .unwrap_or_else(|e| panic!("update of Main#{} failed (seed {}): {:?}", id, seed, e));
                log.push(format!("update Main#{} {}", id, Value::Object(data)));
            }

            // ── update: $increment on a numeric field ───────────────────────────────────────
            7 => {
                let numeric: Vec<&FieldDef> = shape.fields.iter().filter(|f| f.ty.numeric()).collect();
                if numeric.is_empty() {
                    continue;
                }
                let Some(&id) = pick(&mut rng, &shadow.keys().copied().collect::<Vec<_>>()) else { continue };
                let f = numeric[rng.below(numeric.len())];
                let expected = shadow.get_mut(&id).unwrap();
                // $increment on an absent value is a no-op — nothing to add to
                let Some(current) = expected.get(&f.name).and_then(|v| v.as_f64()) else { continue };

                let delta = (rng.next() % 200) as f64 - 100.0;
                // UInt must not go negative and Int must stay in range — the engine rejects those, and a
                // rejection aborts the whole update, which the shadow would then disagree with
                let delta = if f.ty == Ty::UInt && current + delta < 0.0 { delta.abs() } else { delta };
                let new = current + delta;
                let (delta_json, new_json) = if f.ty == Ty::Double {
                    (json!(delta), json!(new))
                } else if f.ty == Ty::UInt {
                    (json!(delta as i64), json!(new as u64))
                } else {
                    (json!(delta as i64), json!(new as i64))
                };
                expected.insert(f.name.clone(), new_json);

                let data = json!({ f.name.clone(): { "$increment": delta_json } });
                try_update(&db, "Main", &json!({ "id": id }), data.clone())
                    .unwrap_or_else(|e| panic!("increment of Main#{}.{} failed (seed {}): {:?}", id, f.name, seed, e));
                log.push(format!("increment Main#{} {}", id, data));
            }

            // ── update: in-place list edits ($push / $pushUnique / $remove) ─────────────────
            8 => {
                let lists: Vec<String> =
                    shape.fields.iter().filter(|f| f.ty == Ty::StrList).map(|f| f.name.clone()).collect();
                if lists.is_empty() {
                    continue;
                }
                let Some(&id) = pick(&mut rng, &shadow.keys().copied().collect::<Vec<_>>()) else { continue };
                let name = lists[rng.below(lists.len())].clone();

                let current: Vec<Value> = shadow[&id][&name].as_array().cloned().unwrap_or_default();
                let fresh: Vec<Value> = (0..1 + rng.below(3)).map(|_| gen_value(&mut rng, Ty::Str)).collect();

                let (op, items, next) = match rng.below(3) {
                    0 => {
                        let mut next = current.clone();
                        next.extend(fresh.iter().cloned());
                        ("$push", fresh, next)
                    }
                    1 => {
                        let mut next = current.clone();
                        for item in fresh.iter() {
                            if !next.contains(item) {
                                next.push(item.clone());
                            }
                        }
                        ("$pushUnique", fresh, next)
                    }
                    _ => {
                        // Removing something actually present is the interesting case, so prefer an
                        // existing element when there is one
                        let victims = if current.is_empty() || rng.chance(25) {
                            fresh
                        } else {
                            vec![current[rng.below(current.len())].clone()]
                        };
                        let next: Vec<Value> = current.iter().filter(|v| !victims.contains(v)).cloned().collect();
                        ("$remove", victims, next)
                    }
                };

                shadow.get_mut(&id).unwrap().insert(name.clone(), Value::Array(next));
                let data = json!({ name.clone(): { op: Value::Array(items) } });
                try_update(&db, "Main", &json!({ "id": id }), data.clone())
                    .unwrap_or_else(|e| panic!("list op on Main#{}.{} failed (seed {}): {:?}", id, name, seed, e));
                log.push(format!("listop Main#{} {}", id, data));
            }

            // ── delete a Target: SetNull (optional relation) or Restrict (required) ─────────
            9 => {
                if targets.len() < 2 {
                    continue;
                }
                let idx = rng.below(targets.len());
                let target = targets[idx].clone();
                let tid = id_of(&target);
                let referenced: Vec<u64> = points_at.iter().filter(|(_, t)| **t == tid).map(|(m, _)| *m).collect();

                let result = try_delete(&db, "Target", target.clone());
                if !shape.rel_optional && !referenced.is_empty() {
                    // Required relation: the delete must be refused and nothing may change
                    match result {
                        Err(DeleteError::RestrictConstraints(ref field, _)) => assert_eq!(field, "Main.rel"),
                        other => panic!("expected Restrict on Target#{} (seed {}), got {:?}", tid, seed, other),
                    }
                    log.push(format!("delete Target#{} → restricted", tid));
                } else {
                    result.unwrap_or_else(|e| panic!("delete of Target#{} failed (seed {}): {:?}", tid, seed, e));
                    targets.remove(idx);
                    for m in referenced.iter() {
                        // THE regression: clearing the foreign key must not disturb any other field
                        shadow.get_mut(m).unwrap().insert("rel".into(), Value::Null);
                        points_at.remove(m);
                    }
                    log.push(format!("delete Target#{} → set null on {:?}", tid, referenced));
                }
            }

            // ── delete a Main row ───────────────────────────────────────────────────────────
            _ => {
                let Some(&id) = pick(&mut rng, &shadow.keys().copied().collect::<Vec<_>>()) else { continue };
                try_delete(&db, "Main", json!({ "id": id }))
                    .unwrap_or_else(|e| panic!("delete of Main#{} failed (seed {}): {:?}", id, seed, e));
                shadow.remove(&id);
                points_at.remove(&id);
                log.push(format!("delete Main#{}", id));
            }
        }

        check(&db, &shape, &shadow, seed, &log);

        // Add a field mid-run, so from here on the table holds a MIX of layouts: rows written before
        // the migration keep the shorter offset table and every later write to them widens first.
        if shape.fields.len() < 14 && rng.chance(5) {
            let entry = migrate_add_field(&mut db, &mut shape, &mut rng, &mut shadow);
            log.push(entry);
            check(&db, &shape, &shadow, seed, &log);
        }

        // Periodically reopen: today's corruption was written to disk, so an in-memory-only check
        // would have declared it healthy. Reading through a fresh open proves the BYTES are right.
        if rng.chance(8) {
            drop(db);
            db = MarciDB::open(&path);
            log.push("── reopen ──".to_string());
            check(&db, &shape, &shadow, seed, &log);
        }
    }
}

fn pick<'a, T>(rng: &mut Rng, items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() { None } else { Some(&items[rng.below(items.len())]) }
}

/// The suite proper. A fixed seed range keeps CI deterministic and makes a failure reproducible by
/// running this test alone — the panic reports the seed, the generated schema, and the last dozen ops.
///
/// The committed range is sized for CI (a few seconds). To hunt for new shapes, widen it and run in
/// release: `cargo test -p marcidb --release --test integration_test row_codec_survives`. Every bug this
/// harness has found so far surfaced within the first few hundred seeds; 3000 × 120 ops (~360k verified
/// operations) currently passes clean.
#[test]
fn row_codec_survives_random_write_sequences() {
    for seed in 1..=60u64 {
        run_seed(seed, 120);
    }
}

/// The bug this harness exists for, pinned as a shape rather than a seed: the foreign key sits in the
/// middle of the row, so clearing it must shift everything after it (fixed in `process_delete::set_null`).
#[test]
fn set_null_with_fields_on_both_sides() {
    let dir = TempDir::new().unwrap();
    let db = MarciDB::new(
        "
        model Target {
            label String
        }
        model Main {
            before  String
            rel     Target?
            after   String
            tail    Int
        }
    ",
        dir.path().to_str().unwrap(),
    );

    let t = insert_data(&db, "Target", json!({ "label": "t" }));
    for i in 0..5 {
        insert_data(&db, "Main", json!({
            "before": "x".repeat(i * 7),
            "rel": t,
            "after": "y".repeat(i * 5),
            "tail": 1000 + i,
        }));
    }

    try_delete(&db, "Target", t).unwrap();

    let rows = get_data(&db, "Main", json!({ "before": true, "rel": true, "after": true, "tail": true }));
    for (i, row) in rows.as_array().unwrap().iter().enumerate() {
        assert_eq!(row["before"], json!("x".repeat(i * 7)), "row {} lost the field before the FK", i);
        assert_eq!(row["rel"], Value::Null);
        assert_eq!(row["after"], json!("y".repeat(i * 5)), "row {} lost the field after the FK", i);
        assert_eq!(row["tail"], json!(1000 + i), "row {} lost the trailing field", i);
    }
}

/// Found by the harness (seed 2). A zero-length value in the row's LAST present slot has
/// `offset == data.len()`: its payload starts exactly where the row ends and is empty. The decoder's
/// bounds check rejected that as `OffsetOutOfRange`, so a perfectly valid row became unreadable —
/// through the JSON path and the binary one alike.
#[test]
fn trailing_empty_value_is_readable() {
    let dir = TempDir::new().unwrap();
    let db = MarciDB::new(
        "
        model M {
            head    Int
            list    String[]
            tail    String?
            unset_a Bool?
            unset_b DateTime?
        }
    ",
        dir.path().to_str().unwrap(),
    );

    // `tail` is empty AND last-present: everything after it is absent, so nothing follows its payload
    insert_data(&db, "M", json!({ "head": 1, "list": ["", "x"], "tail": "" }));
    // ...and the same row shape with a non-empty tail, which always worked
    insert_data(&db, "M", json!({ "head": 2, "list": ["y"], "tail": "z" }));

    let rows = get_data(&db, "M", json!({ "head": true, "list": true, "tail": true, "unset_a": true }));
    assert_eq!(rows, json!([
        { "head": 1, "list": ["", "x"], "tail": "",  "unset_a": null },
        { "head": 2, "list": ["y"],     "tail": "z", "unset_a": null },
    ]));
}

/// Found by the harness. A non-nullable list is typed `string[]` by the generated client, so an absent
/// one must read back as `[]`, not `null` — the engine already treats a missing slot as empty
/// everywhere else. The realistic way to hit this is unavoidable: a migration that adds a list field
/// leaves every pre-existing row without one.
#[test]
fn absent_list_reads_as_empty_not_null() {
    let dir = TempDir::new().unwrap();
    let db = MarciDB::new("model M {\n  tag String\n  items String[]\n  maybe String[]?\n}", dir.path().to_str().unwrap());

    insert_data(&db, "M", json!({ "tag": "explicit", "items": [] }));
    insert_data(&db, "M", json!({ "tag": "omitted" }));

    // A non-nullable list is `[]` however it got there; a nullable one keeps a real null state
    assert_eq!(
        get_data(&db, "M", json!({ "tag": true, "items": true, "maybe": true })),
        json!([
            { "tag": "explicit", "items": [], "maybe": null },
            { "tag": "omitted",  "items": [], "maybe": null },
        ])
    );
}

/// Found by the harness. `reconcile` canonicalizes field order, and the reverse-dependency cache
/// addresses fields in other entities BY INDEX — so reordering invalidated it. The delete planner then
/// read whichever field had landed on the old index. It only bites between a `$sync`/`generate` that
/// moves a field and the next reopen, which is why it never showed up in a normal test.
#[test]
fn field_reorder_keeps_reverse_dependencies_aimed_at_the_right_field() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();

    // `rel` starts first among the body fields
    let mut db = MarciDB::new(
        "
        model Target {
            label String
        }
        model Main {
            rel   Target?
            count Int
        }
    ",
        path,
    );

    let t = insert_data(&db, "Target", json!({ "label": "t" }));
    insert_data(&db, "Main", json!({ "rel": t.clone(), "count": 7 }));

    // Migrate: a new field is declared BEFORE `rel`, so canonicalization moves every later field along
    let mut new_schema = try_parse_schema(
        "
        model Target {
            label String
        }
        model Main {
            added String?
            rel   Target?
            count Int
        }
    ",
    )
    .unwrap();
    reconcile(&mut new_schema, &db.schema);
    let ops = diff(&db.schema, &new_schema).unwrap();
    db.commit_schema(new_schema, &ops).unwrap();

    // The delete plan is built from the freshly reconciled schema — with stale indices this panicked
    // ("rev dependency has wrong type Main.count") instead of clearing the relation
    try_delete(&db, "Target", t).unwrap();

    assert_eq!(
        get_data(&db, "Main", json!({ "rel": { "id": true }, "count": true, "added": true })),
        json!([{ "rel": null, "count": 7, "added": null }])
    );
}
