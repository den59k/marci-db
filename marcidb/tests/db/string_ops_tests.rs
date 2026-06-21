// ─────────────────────────────────────────────────────────────────────────────
// Файл: tests/string_ops_tests.rs
//
// Покрывает непокрытые ветви в process_where.rs:
//   - FieldCompare::StringStartsWith  ($startsWith)
//   - FieldCompare::StringIncludes    ($includes)
//
// Добавить в tests/mod.rs строку:
//   pub mod string_ops_tests;
// ─────────────────────────────────────────────────────────────────────────────

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

fn make_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model User {
            name    String
            email   String?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice",   "email": "alice@example.com"   }));
    insert_data(&db, "User", json!({ "name": "Bob",     "email": "bob@example.com"     }));
    insert_data(&db, "User", json!({ "name": "Charlie", "email": "charlie@example.org" }));
    insert_data(&db, "User", json!({ "name": "Unknown", "email": null                  }));

    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// $startsWith
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_starts_with_test() {
    let (db, _dir) = make_db();

    // email начинается с "alice" → только Alice
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$startsWith": "alice" } }
    }));
    assert_eq!(resp, json!([ { "name": "Alice" } ]));
}

#[test]
fn where_starts_with_multiple_test() {
    let (db, _dir) = make_db();

    // email начинается с "b" → только Bob
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$startsWith": "b" } }
    }));
    assert_eq!(resp, json!([ { "name": "Bob" } ]));
}

#[test]
fn where_starts_with_no_match_test() {
    let (db, _dir) = make_db();

    // email начинается с "zzz" → никто не подходит; null-записи тоже не проходят
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$startsWith": "zzz" } }
    }));
    assert_eq!(resp, json!([]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $includes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_includes_test() {
    let (db, _dir) = make_db();

    // email содержит "example" → Alice, Bob, Charlie (но не Unknown, у которого null)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$includes": "example" } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice"   },
        { "name": "Bob"     },
        { "name": "Charlie" }
    ]));
}

#[test]
fn where_includes_specific_test() {
    let (db, _dir) = make_db();

    // email содержит ".org" → только Charlie
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$includes": ".org" } }
    }));
    assert_eq!(resp, json!([ { "name": "Charlie" } ]));
}

#[test]
fn where_includes_no_match_test() {
    let (db, _dir) = make_db();

    // email содержит "zzz" → никто не подходит
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$includes": "zzz" } }
    }));
    assert_eq!(resp, json!([]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $startsWith и $includes не проходят null-поля
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_string_ops_skip_null_test() {
    let (db, _dir) = make_db();

    // У Unknown поле email = null — он не должен попасть в результат ни для $startsWith, ни для $includes
    let resp_sw = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$startsWith": "" } }
    }));
    // Пустой префикс — все непустые email начинаются с "" (пустой строки),
    // но null-поле всё равно не проходит
    let names: Vec<_> = resp_sw.as_array().unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"Unknown"));
}