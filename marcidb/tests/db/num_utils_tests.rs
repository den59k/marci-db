// tests/db/num_utils_tests.rs
//
// Покрывает num_utils.rs (44.44%) и index_utils.rs (60.74%)
// через интеграционные тесты с @index полями.

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, update_data};

// ─── UInt через @index ────────────────────────────────────────────────────────

#[test]
fn index_sort_uint_gte() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Item {
            name String
            rank UInt @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Item", json!({ "name": "C", "rank": 30 }));
    insert_data(&db, "Item", json!({ "name": "A", "rank": 10 }));
    insert_data(&db, "Item", json!({ "name": "B", "rank": 20 }));

    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "rank": { "$gte": 15 } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
}

#[test]
fn index_sort_uint_lt() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Item {
            name String
            rank UInt @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Item", json!({ "name": "C", "rank": 30 }));
    insert_data(&db, "Item", json!({ "name": "A", "rank": 10 }));
    insert_data(&db, "Item", json!({ "name": "B", "rank": 20 }));

    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "rank": { "$lt": 15 } }
    }));
    assert_eq!(resp, json!([{ "name": "A" }]));
}

#[test]
fn index_sort_uint_lte() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Item {
            name String
            rank UInt @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Item", json!({ "name": "A", "rank": 10 }));
    insert_data(&db, "Item", json!({ "name": "B", "rank": 20 }));
    insert_data(&db, "Item", json!({ "name": "C", "rank": 30 }));

    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "rank": { "$lte": 20 } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
}

#[test]
fn index_sort_uint_eq() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Item {
            name String
            rank UInt @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Item", json!({ "name": "A", "rank": 10 }));
    insert_data(&db, "Item", json!({ "name": "B", "rank": 20 }));
    insert_data(&db, "Item", json!({ "name": "C", "rank": 20 }));

    let resp = get_data(&db, "Item", json!({
        "name": true,
        "$where": { "rank": { "$eq": 20 } }
    }));
    assert_eq!(resp.as_array().unwrap().len(), 2);
}

#[test]
fn index_uint_boundary_zero() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model X {
            name String
            val  UInt @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "X", json!({ "name": "zero", "val": 0 }));
    insert_data(&db, "X", json!({ "name": "one",  "val": 1 }));

    let resp = get_data(&db, "X", json!({
        "name": true,
        "$where": { "val": { "$lte": 0 } }
    }));
    assert_eq!(resp, json!([{ "name": "zero" }]));
}

// ─── Int (отрицательные) через @index ────────────────────────────────────────

#[test]
fn index_sort_int_negative() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Score {
            name String
            val  Int @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Score", json!({ "name": "pos",  "val":  10 }));
    insert_data(&db, "Score", json!({ "name": "neg",  "val": -5  }));
    insert_data(&db, "Score", json!({ "name": "zero", "val":  0  }));

    let resp = get_data(&db, "Score", json!({
        "name": true,
        "$where": { "val": { "$lt": 0 } }
    }));
    assert_eq!(resp, json!([{ "name": "neg" }]));
}

#[test]
fn index_sort_int_gte_zero() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Score {
            name String
            val  Int @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Score", json!({ "name": "pos",  "val":  10 }));
    insert_data(&db, "Score", json!({ "name": "neg",  "val": -5  }));
    insert_data(&db, "Score", json!({ "name": "zero", "val":  0  }));

    let resp = get_data(&db, "Score", json!({
        "name": true,
        "$where": { "val": { "$gte": 0 } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"pos"));
    assert!(names.contains(&"zero"));
    assert!(!names.contains(&"neg"));
}

#[test]
fn index_sort_int_negative_range() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Score {
            name String
            val  Int @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Score", json!({ "name": "a", "val": -100 }));
    insert_data(&db, "Score", json!({ "name": "b", "val": -50  }));
    insert_data(&db, "Score", json!({ "name": "c", "val": -1   }));

    let resp = get_data(&db, "Score", json!({
        "name": true,
        "$where": { "val": { "$lt": -10 } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
    assert!(!names.contains(&"c"));
}

#[test]
fn index_int_boundary_min_negative() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model X {
            name String
            val  Int @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "X", json!({ "name": "big_neg", "val": -1000000 }));
    insert_data(&db, "X", json!({ "name": "pos",     "val": 1        }));

    let resp = get_data(&db, "X", json!({
        "name": true,
        "$where": { "val": { "$lt": -10 } }
    }));
    assert_eq!(resp, json!([{ "name": "big_neg" }]));
}

// ─── Float через @index ───────────────────────────────────────────────────────

#[test]
fn index_sort_float_gt() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Metric {
            label  String
            rating Float @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Metric", json!({ "label": "high", "rating": 9.5  }));
    insert_data(&db, "Metric", json!({ "label": "low",  "rating": -1.0 }));
    insert_data(&db, "Metric", json!({ "label": "mid",  "rating": 5.0  }));

    let resp = get_data(&db, "Metric", json!({
        "label": true,
        "$where": { "rating": { "$gt": 4.0 } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let labels: Vec<&str> = arr.iter().map(|v| v["label"].as_str().unwrap()).collect();
    assert!(labels.contains(&"high"));
    assert!(labels.contains(&"mid"));
}

#[test]
fn index_sort_float_negative_lt() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Metric {
            label  String
            rating Float @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Metric", json!({ "label": "neg_big",   "rating": -100.0 }));
    insert_data(&db, "Metric", json!({ "label": "neg_small", "rating": -1.0   }));
    insert_data(&db, "Metric", json!({ "label": "positive",  "rating": 5.0    }));

    let resp = get_data(&db, "Metric", json!({
        "label": true,
        "$where": { "rating": { "$lt": -10.0 } }
    }));
    assert_eq!(resp, json!([{ "label": "neg_big" }]));
}

// ─── Double через @index ──────────────────────────────────────────────────────

#[test]
fn index_sort_double_gte() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Meas {
            name  String
            value Double @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Meas", json!({ "name": "pi", "value": 3.14159265358979 }));
    insert_data(&db, "Meas", json!({ "name": "e",  "value": 2.71828182845905 }));
    insert_data(&db, "Meas", json!({ "name": "sq", "value": 1.41421356237310 }));

    let resp = get_data(&db, "Meas", json!({
        "name": true,
        "$where": { "value": { "$gte": 3.0 } }
    }));
    assert_eq!(resp, json!([{ "name": "pi" }]));
}

#[test]
fn index_sort_double_negative_range() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Meas {
            name  String
            value Double @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Meas", json!({ "name": "a", "value": -1000.0 }));
    insert_data(&db, "Meas", json!({ "name": "b", "value": -0.5    }));
    insert_data(&db, "Meas", json!({ "name": "c", "value":  0.0    }));

    let resp = get_data(&db, "Meas", json!({
        "name": true,
        "$where": { "value": { "$lte": -0.5 } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
    assert!(!names.contains(&"c"));
}

// ─── DateTime через @index ────────────────────────────────────────────────────

#[test]
fn index_sort_datetime_lt() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Event {
            name String
            ts   DateTime @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Event", json!({ "name": "old", "ts": 1000_i64 }));
    insert_data(&db, "Event", json!({ "name": "new", "ts": 9000_i64 }));
    insert_data(&db, "Event", json!({ "name": "mid", "ts": 5000_i64 }));

    let resp = get_data(&db, "Event", json!({
        "name": true,
        "$where": { "ts": { "$lt": 3000_i64 } }
    }));
    assert_eq!(resp, json!([{ "name": "old" }]));
}

#[test]
fn index_sort_datetime_range() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Event {
            name String
            ts   DateTime @index
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Event", json!({ "name": "a", "ts": 1000_i64 }));
    insert_data(&db, "Event", json!({ "name": "b", "ts": 3000_i64 }));
    insert_data(&db, "Event", json!({ "name": "c", "ts": 9000_i64 }));

    let resp = get_data(&db, "Event", json!({
        "name": true,
        "$where": { "$and": [
            { "ts": { "$gte": 1000_i64 } },
            { "ts": { "$lte": 5000_i64 } }
        ] }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
    assert!(!names.contains(&"c"));
}

// ─── @unique (строковый индекс) ───────────────────────────────────────────────

#[test]
fn index_unique_string_lookup() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model User {
            name  String
            email String @unique
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "User", json!({ "name": "Alice", "email": "a@test.com" }));
    insert_data(&db, "User", json!({ "name": "Bob",   "email": "b@test.com" }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": "a@test.com" }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

// ─── $startsWith через строковый @index ───────────────────────────────────────

#[test]
fn index_starts_with_string() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model User {
            name  String @index
            email String?
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "User", json!({ "name": "Alice", "email": "a@test.com" }));
    insert_data(&db, "User", json!({ "name": "Alex",  "email": "alex@test.com" }));
    insert_data(&db, "User", json!({ "name": "Bob",   "email": "b@test.com" }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$startsWith": "Al" } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Alex"));
}

// ─── $increment на @index полях (NumberValue::increment_bytes) ────────────────

#[test]
fn increment_indexed_uint_field() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Counter {
            label String
            count UInt @index
        }
    ", dir.path().to_str().unwrap());
    let id = insert_data(&db, "Counter", json!({ "label": "hits", "count": 10 }));
    update_data(&db, "Counter", &id, json!({ "count": { "$increment": 5 } }));

    let resp = get_data(&db, "Counter", json!({
        "label": true,
        "count": true,
        "$where": { "count": { "$gte": 15 } }
    }));
    assert_eq!(resp, json!([{ "label": "hits", "count": 15 }]));
}

#[test]
fn increment_indexed_int_field_negative() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Counter {
            label String
            delta Int @index
        }
    ", dir.path().to_str().unwrap());
    let id = insert_data(&db, "Counter", json!({ "label": "x", "delta": 0 }));
    update_data(&db, "Counter", &id, json!({ "delta": { "$increment": -10 } }));

    let resp = get_data(&db, "Counter", json!({
        "label": true,
        "$where": { "delta": { "$eq": -10 } }
    }));
    assert_eq!(resp, json!([{ "label": "x" }]));
}