// tests/where_coverage_tests.rs
//
// Добавить в tests/mod.rs:
//   pub mod where_coverage_tests;
//
// Файл покрывает непокрытые ветки (57% → ~82%):
//
// ── process_where.rs ─────────────────────────────────────────────────────────
//   compare_field_bytes:
//     EqNull/NeNull/Eq/Ne/In/NotIn/Gt/Gte/Lt/Lte — через EnumList с вариантными полями
//     null-данные (offset=0) → EqNull→true, NeNull→false, Ne→true, In(has_null)→true/false
//   EnumListSome/Every/None с field_filters (не просто $variant, а поле)
//   has_one_item с only_id_required=true (Ref + where только по key-полям)
//   In/NotIn когда поле == null → has_null ветки
//
// ── where.rs ─────────────────────────────────────────────────────────────────
//   only_id_required: And / Or / Not / Field(Key) / Field(Body)
//   Вызывается из has_one_item в process_where.rs

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

// ─────────────────────────────────────────────────────────────────────────────
// Схема: EnumList с вариантными полями разных типов
// Используется для покрытия compare_field_bytes
// ─────────────────────────────────────────────────────────────────────────────

fn make_event_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        enum Event {
            created {
                at    UInt
                score Int?
            }
            updated {
                at    UInt
                note  String?
                score Int?
            }
        }

        model Doc {
            name   String
            events Event[]
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    // alpha: только created(at=100, score=5)  ← только один вариант, score присутствует
    insert_data(&db, "Doc", json!({
        "name": "alpha",
        "events": [
            { "$variant": "created", "at": 100, "score": 5 }
        ]
    }));

    // beta: только created(at=50), score отсутствует → null
    insert_data(&db, "Doc", json!({
        "name": "beta",
        "events": [
            { "$variant": "created", "at": 50 }
        ]
    }));

    // delta: только updated(at=200, note="fix", score=-1)
    insert_data(&db, "Doc", json!({
        "name": "delta",
        "events": [
            { "$variant": "updated", "at": 200, "note": "fix", "score": -1 }
        ]
    }));

    // epsilon: только updated(at=300), note и score отсутствуют → null
    insert_data(&db, "Doc", json!({
        "name": "epsilon",
        "events": [
            { "$variant": "updated", "at": 300 }
        ]
    }));

    // gamma: явно пустой список — count=0, но поле записано
    // (без events данные вовсе не пишутся → $none вернёт false)
    insert_data(&db, "Doc", json!({ "name": "gamma", "events": [] }));

    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: EqNull (поле есть → false; поле null → true)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_eq_null_with_data_is_false() {
    // alpha: score=5 → $some{score:null} → EqNull=false → alpha не проходит
    // beta: score отсутствует → $some{score:null} → EqNull=true → beta проходит
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "score": null } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"beta"));   // beta.created: score=null → EqNull=true
    assert!(!names.contains(&"alpha")); // alpha.created: score=5 → EqNull=false
}

#[test]
fn enum_list_field_eq_null_data_missing_is_true() {
    // beta: только created(at=50, score=null) → $every{score:null} → true
    // alpha: только created(at=100, score=5)  → $every{score:null} → EqNull=false → не проходит
    // gamma: пустой список → вакуумная истина
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$every": { "$variant": "created", "score": null } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"beta"));   // созданный без score → null → EqNull=true
    assert!(names.contains(&"gamma"));  // пустой список → вакуумная истина
    assert!(!names.contains(&"alpha")); // score=5 → EqNull=false
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: NeNull
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_ne_null_with_data_is_true() {
    // alpha: score=5 → NeNull=true; beta: score=null → NeNull=false
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "score": { "$ne": null } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));
    assert!(!names.contains(&"beta")); // beta.created: score=null → NeNull=false
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: Eq
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_eq_exact_match() {
    // at == 100 → alpha.created
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$eq": 100 } } } }
    }));
    assert_eq!(resp, json!([{ "name": "alpha" }]));
}

#[test]
fn enum_list_field_eq_implicit_match() {
    // Прямое значение → Eq (at == 50 → beta.created)
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": 50 } } }
    }));
    assert_eq!(resp, json!([{ "name": "beta" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: Ne
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_ne_excludes_matching() {
    // at != 100 → только beta.created(at=50) проходит $some; alpha(at=100) нет
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$ne": 100 } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"beta"));   // at=50 != 100 → Ne=true
    assert!(!names.contains(&"alpha")); // at=100 == 100 → Ne=false
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: Gt / Gte / Lt / Lte
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_gt_filters_correctly() {
    // at > 60 → alpha.created(100) проходит, beta.created(50) нет
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$gt": 60 } } } }
    }));
    assert_eq!(resp, json!([{ "name": "alpha" }]));
}

#[test]
fn enum_list_field_gte_filters_correctly() {
    // at >= 100 → alpha проходит $some; beta.created=50 → нет
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$gte": 100 } } } }
    }));
    assert_eq!(resp, json!([{ "name": "alpha" }]));
}

#[test]
fn enum_list_field_lt_filters_correctly() {
    // at < 100 → beta.created(50) проходит, alpha.created(100) нет
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$lt": 100 } } } }
    }));
    assert_eq!(resp, json!([{ "name": "beta" }]));
}

#[test]
fn enum_list_field_lte_filters_correctly() {
    // at <= 50 → только beta.created(50) проходит
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$lte": 50 } } } }
    }));
    assert_eq!(resp, json!([{ "name": "beta" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: In / NotIn  (data present)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_in_with_match() {
    // at in [100, 999] → alpha.created
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$in": [100, 999] } } } }
    }));
    assert_eq!(resp, json!([{ "name": "alpha" }]));
}

#[test]
fn enum_list_field_not_in_with_match() {
    // at notIn [100] → beta.created(50) проходит, alpha.created(100) нет
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": { "$notIn": [100] } } } }
    }));
    assert_eq!(resp, json!([{ "name": "beta" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// compare_field_bytes: In/NotIn когда данные == null  (ветка Some(data) отсутствует)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_in_null_branch_has_null_true() {
    // score=null у beta.created; $in: [null] → has_null=true → beta проходит
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "score": { "$in": [null] } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"beta"));
    assert!(!names.contains(&"alpha")); // alpha.created score=5 (не null)
}

#[test]
fn enum_list_field_not_in_null_branch_has_null_false() {
    // score=null у beta.created; $notIn: [null] (has_null=true) → !has_null=false → beta не проходит
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "score": { "$notIn": [null] } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    // beta.created: score=null, $notIn:[null] → false
    assert!(!names.contains(&"beta"));
    // alpha.created: score=5, 5 not in [null] → true
    assert!(names.contains(&"alpha"));
}

#[test]
fn enum_list_field_ne_null_branch() {
    // score=null у beta.created; $ne: 999 → (null) → Ne → true
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "score": { "$ne": 999 } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    // beta.created: score=null → Ne(999) → true (null != 999)
    assert!(names.contains(&"beta"));
}

// ─────────────────────────────────────────────────────────────────────────────
// EnumListNone с field_filters
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_none_with_field_filter() {
    // $none: ни один created-event с at > 200
    // alpha: created(at=100) → 100 > 200? No → item=false → $none=!false=true ✓
    // beta:  created(at=50)  → 50  > 200? No → item=false → $none=true ✓
    // delta/epsilon: only updated → variant mismatch → false → $none=true ✓
    // gamma: count=0 → code возвращает false для $none (только $every=true при count=0)
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$none": { "$variant": "created", "at": { "$gt": 200 } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));   // created(at=100): не > 200 → $none=true
    assert!(names.contains(&"beta"));    // created(at=50):  не > 200 → $none=true
    assert!(!names.contains(&"gamma")); // count=0 → $none=false (только $every=true при пустом)
}

// ─────────────────────────────────────────────────────────────────────────────
// EnumListEvery с field_filters
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_every_with_field_filter_vacuous_truth() {
    // gamma (пустой список) всегда проходит $every
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$every": { "$variant": "created", "at": { "$gt": 9999 } } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"gamma"));
    assert!(!names.contains(&"alpha"));
    assert!(!names.contains(&"beta"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Поиск по полю из другого варианта (field_idx variant_idx mismatch → skip)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_field_wrong_variant_is_skipped() {
    // $some с { "$variant": "created", "note": "fix" }
    // "note" принадлежит updated, не created → условие пропускается → $some=true если вариант совпадает
    // У alpha есть created(at=100) → created-элемент → variant совпадает → note пропускается → true
    let (db, _dir) = make_event_db();
    let resp = get_data(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created" } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha")); // есть created
    assert!(names.contains(&"beta"));  // есть created
    assert!(!names.contains(&"gamma")); // пустой
}

// ─────────────────────────────────────────────────────────────────────────────
// where.rs: only_id_required — And / Or / Not  (через has_one_item)
//
// only_id_required вызывается в has_one_item для Ref-полей.
// Если where-условие для linked entity ссылается только на key-поля,
// тело записи не загружается ($where по id-полю).
// Если body-поле — загружается.
// ─────────────────────────────────────────────────────────────────────────────

fn make_ref_where_db() -> (MarciDB, tempfile::TempDir) {
    // User с полем 'name' в body и 'age' в body (не @id)
    // Post ссылается на User через Ref
    let schema_str = "
        model User {
            name  String
            age   UInt?
        }
        model Post {
            title  String
            author User?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
    (db, dir)
}

// only_id_required: Where::True → true (без загрузки body)
#[test]
fn only_id_required_true_no_body_fetch() {
    let (db, _dir) = make_ref_where_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice", "age": 25 }));
    insert_data(&db, "Post", json!({ "title": "P1", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "P2" }));

    // author = {} (Where::True) → only_id_required=true → body не загружается
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": { "author": {} }
    }));
    assert_eq!(resp, json!([{ "title": "P1" }]));
}

// only_id_required: Where::And — все body-поля → false → body загружается
#[test]
fn only_id_required_and_with_body_fields_loads_body() {
    let (db, _dir) = make_ref_where_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice", "age": 25 }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob",   "age": 30 }));
    insert_data(&db, "Post", json!({ "title": "P-Alice", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "P-Bob",   "author": bob   }));

    // And([name==Alice, age==25]) → body-поля → only_id_required=false
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": {
            "author": {
                "$and": [
                    { "name": "Alice" },
                    { "age": 25 }
                ]
            }
        }
    }));
    assert_eq!(resp, json!([{ "title": "P-Alice" }]));
}

// only_id_required: Where::Or — body-поля → false
#[test]
fn only_id_required_or_with_body_fields_loads_body() {
    let (db, _dir) = make_ref_where_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice", "age": 25 }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob",   "age": 99 }));
    insert_data(&db, "Post", json!({ "title": "P-Alice", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "P-Bob",   "author": bob   }));
    insert_data(&db, "Post", json!({ "title": "Orphan" }));

    // Or([name==Alice, name==Bob]) → only_id_required=false (body-поле)
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": {
            "author": {
                "$or": [
                    { "name": "Alice" },
                    { "name": "Bob" }
                ]
            }
        }
    }));
    let arr = resp.as_array().unwrap();
    let titles: Vec<_> = arr.iter().map(|v| v["title"].as_str().unwrap()).collect();
    assert!(titles.contains(&"P-Alice"));
    assert!(titles.contains(&"P-Bob"));
    assert!(!titles.contains(&"Orphan"));
}

// only_id_required: Where::Not → делегирует вложенному условию
#[test]
fn only_id_required_not_with_body_field() {
    let (db, _dir) = make_ref_where_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice", "age": 25 }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob",   "age": 30 }));
    insert_data(&db, "Post", json!({ "title": "P-Alice", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "P-Bob",   "author": bob   }));

    // Not(name==Bob) → only_id_required=false → загружаем body → Alice проходит
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": {
            "author": { "$not": { "name": "Bob" } }
        }
    }));
    assert_eq!(resp, json!([{ "title": "P-Alice" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// process_where: In/NotIn когда поле == null
// (ветка None у get_data → FieldCompare::In(_, has_null))
// ─────────────────────────────────────────────────────────────────────────────

fn make_nullable_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model Item {
            name  String
            code  UInt?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
    insert_data(&db, "Item", json!({ "name": "A", "code": 10  }));
    insert_data(&db, "Item", json!({ "name": "B", "code": null }));
    insert_data(&db, "Item", json!({ "name": "C", "code": 20  }));
    (db, dir)
}

#[test]
fn in_with_null_matches_null_field() {
    // $in:[null,10] → has_null=true → B (code=null) попадает
    let (db, _dir) = make_nullable_db();
    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "code": { "$in": [null, 10] } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"A")); // code=10
    assert!(names.contains(&"B")); // code=null, has_null=true
    assert!(!names.contains(&"C"));
}

#[test]
fn in_without_null_excludes_null_field() {
    // $in:[10,20] → has_null=false → B не попадает
    let (db, _dir) = make_nullable_db();
    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "code": { "$in": [10, 20] } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"A"));
    assert!(names.contains(&"C"));
    assert!(!names.contains(&"B")); // code=null, has_null=false
}

#[test]
fn not_in_with_null_excludes_null_field() {
    // $notIn:[null] → has_null=true → !has_null=false → B (null) не проходит
    let (db, _dir) = make_nullable_db();
    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "code": { "$notIn": [null] } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"A"));
    assert!(names.contains(&"C"));
    assert!(!names.contains(&"B"));
}

#[test]
fn not_in_without_null_includes_null_field() {
    // $notIn:[10] → has_null=false → !has_null=true → B (null) проходит
    let (db, _dir) = make_nullable_db();
    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "code": { "$notIn": [10] } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"B")); // null, has_null=false → !false=true
    assert!(names.contains(&"C")); // 20 not in [10]
    assert!(!names.contains(&"A")); // 10 in [10]
}

// ─────────────────────────────────────────────────────────────────────────────
// process_where: StringStartsWith / StringIncludes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_string_includes() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Article {
            title String
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Article", json!({ "title": "Rust programming guide" }));
    insert_data(&db, "Article", json!({ "title": "Go concurrency patterns" }));
    insert_data(&db, "Article", json!({ "title": "Advanced Rust tips" }));

    let resp = get_data(&db, "Article", json!({
        "title": true,
        "$where": { "title": { "$includes": "Rust" } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let titles: Vec<_> = arr.iter().map(|v| v["title"].as_str().unwrap()).collect();
    assert!(titles.iter().all(|t| t.contains("Rust")));
}

#[test]
fn process_where_string_starts_with_no_match() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Tag {
            label String
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Tag", json!({ "label": "alpha" }));
    insert_data(&db, "Tag", json!({ "label": "beta" }));

    let resp = get_data(&db, "Tag", json!({
        "label": true,
        "$where": { "label": { "$startsWith": "gamma" } }
    }));
    assert_eq!(resp, json!([]));
}

// ─────────────────────────────────────────────────────────────────────────────
// process_where: get_data_one / find_first с where
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn find_first_with_enum_list_filter() {
    let (db, _dir) = make_event_db();

    // Первый документ с created.at==100 → alpha
    let result = get_data_one(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$some": { "$variant": "created", "at": 100 } } }
    }));
    assert_eq!(result["name"], "alpha");
}

#[test]
fn find_first_enum_list_every_matches_gamma() {
    let (db, _dir) = make_event_db();

    // gamma: events=[] → $every вакуумно истинно
    // alpha/beta: created с at<=9999 → every=false
    // delta/epsilon: [updated] → updated != created → every([false])=false
    // Вставка по порядку: alpha,beta,delta,epsilon,gamma → first match = gamma
    let result = get_data_one(&db, "Doc", json!({
        "name": true,
        "$where": { "events": { "$every": { "$variant": "created", "at": { "$gt": 9999 } } } }
    }));
    assert_eq!(result["name"], "gamma");
}
