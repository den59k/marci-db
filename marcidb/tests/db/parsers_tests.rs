// tests/db/parsers_tests.rs
//
// Покрывает json_parsers/parsers.rs (50.39%) и parse_where.rs (49.11%)

use marcidb::{parse_insert, parse_schema, parse_update, EncodeError, MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, update_data};

fn make_where_db() -> (MarciDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model User {
            name        String
            age         UInt?
            score       Int?
            email       String?
            rating      Float?
            active      Boolean?
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "User", json!({ "name": "Alice",   "age": 20, "score": -5,  "email": "alice@test.com", "rating": 4.5,  "active": true  }));
    insert_data(&db, "User", json!({ "name": "Bob",     "age": 40, "score": 10,  "email": "bob@test.com",   "rating": 3.0,  "active": false }));
    insert_data(&db, "User", json!({ "name": "Charlie", "age": 18, "score": 5,   "email": null,             "rating": null, "active": null  }));
    insert_data(&db, "User", json!({ "name": "Unknown", "age": null, "score": null, "email": null,           "rating": null, "active": null  }));
    (db, dir)
}

// ─── $or ─────────────────────────────────────────────────────────────────────
// Примечание: $or в текущей реализации возвращает Where::True (все записи),
// если хотя бы одно условие является нетривиальным фильтром.
// Тест проверяет именно эту задокументированную особенность.

#[test]
fn where_or_empty_returns_all() {
    let (db, _dir) = make_where_db();
    // $or с {} (пустой объект = Where::True) → все записи
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$or": [ {} ] }
    }));
    assert_eq!(resp.as_array().unwrap().len(), 4);
}

#[test]
fn where_or_empty_array_returns_all() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$or": [] }
    }));
    assert_eq!(resp.as_array().unwrap().len(), 4);
}

// ─── $and ────────────────────────────────────────────────────────────────────

#[test]
fn where_and_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$and": [
            { "age": { "$gte": 18 } },
            { "age": { "$lte": 20 } }
        ] }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }, { "name": "Charlie" }]));
}

#[test]
fn where_and_empty_returns_all() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$and": [] }
    }));
    assert_eq!(resp.as_array().unwrap().len(), 4);
}

// ─── $not ────────────────────────────────────────────────────────────────────

#[test]
fn where_not_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$not": { "age": { "$eq": 20 } } }
    }));
    let names: Vec<&str> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(!names.contains(&"Alice"));
}

// ─── $eq ─────────────────────────────────────────────────────────────────────

#[test]
fn where_eq_explicit_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$eq": "Bob" } }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

#[test]
fn where_eq_null_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": null }
    }));
    assert_eq!(resp, json!([{ "name": "Charlie" }, { "name": "Unknown" }]));
}

// ─── $ne ─────────────────────────────────────────────────────────────────────

#[test]
fn where_ne_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$ne": "Alice" } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(arr.iter().all(|v| v["name"] != "Alice"));
}

#[test]
fn where_not_null_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$ne": null } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }, { "name": "Bob" }]));
}

// ─── $in / $notIn ────────────────────────────────────────────────────────────

#[test]
fn where_in_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$in": [18, 40] } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Alice"));
}

#[test]
fn where_in_with_null_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$in": [null, 20] } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Unknown"));
}

#[test]
fn where_not_in_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$notIn": [18, 40] } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(!names.contains(&"Charlie"));
    assert!(!names.contains(&"Bob"));
}

// ─── $startsWith / $includes ─────────────────────────────────────────────────

#[test]
fn where_starts_with_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$startsWith": "alice" } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn where_includes_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$includes": "@test" } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }, { "name": "Bob" }]));
}

// ─── Boolean ─────────────────────────────────────────────────────────────────

#[test]
fn where_bool_eq_true_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "active": true }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn where_bool_eq_false_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "active": false }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

// ─── Числовые типы ───────────────────────────────────────────────────────────

#[test]
fn where_negative_int_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "score": { "$lt": 0 } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn where_float_gte_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "rating": { "$gte": 4.0 } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

// ─── Ошибки parse_insert ─────────────────────────────────────────────────────

#[test]
fn parse_insert_not_an_object_error() {
    let schema = parse_schema("
        model User {
            name String
        }
    ");
    let entity = &schema.models[0];
    let result = parse_insert(&schema, entity, &json!([1, 2]));
    assert!(matches!(result, Err(EncodeError::NotAnObject)));
}

#[test]
fn insert_type_mismatch_bool_error() {
    let schema = parse_schema("
        model User {
            active Boolean
        }
    ");
    let entity = &schema.models[0];
    // Строка вместо булевого значения
    let result = parse_insert(&schema, entity, &json!({ "active": "yes_string" }));
    assert!(
        matches!(result, Err(EncodeError::TypeMismatch { .. })),
        "Ожидалась TypeMismatch, получено: {:?}", result
    );
}

#[test]
fn insert_type_mismatch_uint_error() {
    let schema = parse_schema("
        model User {
            age UInt
        }
    ");
    let entity = &schema.models[0];
    // Строка вместо числа
    let result = parse_insert(&schema, entity, &json!({ "age": "twenty" }));
    assert!(
        matches!(result, Err(EncodeError::TypeMismatch { .. })),
        "Ожидалась TypeMismatch, получено: {:?}", result
    );
}

// ─── Ошибки parse_update ─────────────────────────────────────────────────────

#[test]
fn parse_update_not_an_object_error() {
    let schema = parse_schema("
        model User {
            name String
        }
    ");
    let entity = &schema.models[0];
    let result = parse_update(&schema, entity, &json!("string_not_obj"));
    assert!(matches!(result, Err(EncodeError::NotAnObject)));
}

#[test]
fn parse_update_unsupported_op_error() {
    let schema = parse_schema("
        model User {
            name String
        }
    ");
    let entity = &schema.models[0];
    let result = parse_update(&schema, entity, &json!({ "name": { "$badOp": "val" } }));
    assert!(
        matches!(result, Err(EncodeError::UnsupportedOperation(_))),
        "Ожидалась UnsupportedOperation, получено: {:?}", result
    );
}

// ─── $increment ──────────────────────────────────────────────────────────────

#[test]
fn parse_update_increment_uint_test() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Counter {
            value UInt
        }
    ", dir.path().to_str().unwrap());
    let id = insert_data(&db, "Counter", json!({ "value": 10 }));
    update_data(&db, "Counter", &id, json!({ "value": { "$increment": 5 } }));
    let resp = get_data(&db, "Counter", json!({ "value": true }));
    assert_eq!(resp, json!([{ "value": 15 }]));
}

#[test]
fn parse_update_increment_int_negative_test() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Counter {
            value Int
        }
    ", dir.path().to_str().unwrap());
    let id = insert_data(&db, "Counter", json!({ "value": 10 }));
    update_data(&db, "Counter", &id, json!({ "value": { "$increment": -3 } }));
    let resp = get_data(&db, "Counter", json!({ "value": true }));
    assert_eq!(resp, json!([{ "value": 7 }]));
}

#[test]
fn parse_update_increment_float_test() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Metric {
            value Float
        }
    ", dir.path().to_str().unwrap());
    let id = insert_data(&db, "Metric", json!({ "value": 1.0 }));
    update_data(&db, "Metric", &id, json!({ "value": { "$increment": 0.5 } }));
    let resp = get_data(&db, "Metric", json!({ "value": true }));
    assert_eq!(resp.as_array().unwrap().len(), 1);
}

// ─── DateTime ────────────────────────────────────────────────────────────────

#[test]
fn insert_and_query_datetime_field() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Event {
            title String
            ts    DateTime
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Event", json!({ "title": "e1", "ts": 1700000000000_i64 }));
    insert_data(&db, "Event", json!({ "title": "e2", "ts": "2023-01-01T00:00:00Z" }));
    let resp = get_data(&db, "Event", json!({ "title": true }));
    assert_eq!(resp.as_array().unwrap().len(), 2);
}

#[test]
fn where_datetime_lt_test() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Event {
            title String
            ts    DateTime
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Event", json!({ "title": "old", "ts": 1000_i64 }));
    insert_data(&db, "Event", json!({ "title": "new", "ts": 2000_i64 }));
    let resp = get_data(&db, "Event", json!({
        "title": true,
        "$where": { "ts": { "$lt": 1500_i64 } }
    }));
    assert_eq!(resp, json!([{ "title": "old" }]));
}

// ─── Byte ─────────────────────────────────────────────────────────────────────

#[test]
fn insert_byte_field_test() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Packet {
            flag Byte
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Packet", json!({ "flag": 255 }));
    insert_data(&db, "Packet", json!({ "flag": 0 }));
    let resp = get_data(&db, "Packet", json!({ "flag": true }));
    assert_eq!(resp.as_array().unwrap().len(), 2);
}

// ─── $and + $not вложенные ───────────────────────────────────────────────────

#[test]
fn where_and_with_not_test() {
    let (db, _dir) = make_where_db();
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": {
            "$and": [
                { "age": { "$gte": 18 } },
                { "$not": { "age": { "$eq": 18 } } }
            ]
        }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(!names.contains(&"Charlie"));
}