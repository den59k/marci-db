// tests/process_where_tests.rs
//
// Покрывает:
//   - query_op/process_where.rs (50.83%)
//   - query_op/process_query_one.rs (82.50%)
//   - query_op/where.rs (45.45%)
//
// Тестируются непокрытые ветки:
//   - FieldCompare::In / NotIn / EqNull / NeNull / Gt / Gte / Lt / Lte
//   - FieldCompare::Ref -> Exists / NotExists
//   - Where::Or / Where::Not
//   - find_first / get_data_one

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data, update_data};

// ─────────────────────────────────────────────────────────────────────────────
// Схема для большинства тестов
// ─────────────────────────────────────────────────────────────────────────────
fn make_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model User {
            name        String
            age         UInt?
            score       Int?
            active      Boolean?
            info        UserInfo?
            posts       Post[]      @bind(Post.author)
        }

        struct UserInfo {
            bio     String
            level   Int
        }

        model Post {
            title   String
            author  User?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// find_first / get_data_one
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn find_first_returns_one_record() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "age": 20 }));
    insert_data(&db, "User", json!({ "name": "Bob",   "age": 30 }));

    let result = get_data_one(&db, "User", json!({ "name": true, "$where": { "name": "Alice" } }));
    assert_eq!(result, json!({ "name": "Alice" }));
}

#[test]
fn find_first_returns_null_when_not_found() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice" }));

    let result = get_data_one(&db, "User", json!({ "name": true, "$where": { "name": "Nobody" } }));
    assert_eq!(result, json!(null));
}

#[test]
fn find_first_no_where_returns_first() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob" }));

    let result = get_data_one(&db, "User", json!({ "name": true }));
    assert_eq!(result, json!({ "name": "Alice" }));
}

// ─────────────────────────────────────────────────────────────────────────────
// FieldCompare::In / NotIn
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_in_uint() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice",   "age": 20 }));
    insert_data(&db, "User", json!({ "name": "Bob",     "age": 40 }));
    insert_data(&db, "User", json!({ "name": "Charlie", "age": 18 }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$in": [20, 18] } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Bob"));
}

#[test]
fn process_where_not_in_uint() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice",   "age": 20 }));
    insert_data(&db, "User", json!({ "name": "Bob",     "age": 40 }));
    insert_data(&db, "User", json!({ "name": "Charlie", "age": 18 }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$notIn": [20, 18] } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Bob"));
    assert!(!names.contains(&"Alice"));
    assert!(!names.contains(&"Charlie"));
}

#[test]
fn process_where_in_string() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob"   }));
    insert_data(&db, "User", json!({ "name": "Charlie" }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$in": ["Alice", "Charlie"] } }
    }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// EqNull / NeNull на nullable полях
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_eq_null_uint() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "age": 20  }));
    insert_data(&db, "User", json!({ "name": "Bob",   "age": null }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": null }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

#[test]
fn process_where_ne_null_uint() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "age": 20  }));
    insert_data(&db, "User", json!({ "name": "Bob",   "age": null }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$ne": null } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Ref поля — Exists / NotExists
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_ref_exists() {
    let (db, _dir) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob" }));
    insert_data(&db, "Post", json!({ "title": "Post1", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "Post2" }));

    // author не null → Post1
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": { "author": { "$ne": null } }
    }));
    assert_eq!(resp, json!([{ "title": "Post1" }]));
}

#[test]
fn process_where_ref_not_exists() {
    let (db, _dir) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "Post", json!({ "title": "Post1", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "Orphan" }));

    // author == null → Orphan
    let resp = get_data(&db, "Post", json!({
        "title": true,
        "$where": { "author": null }
    }));
    assert_eq!(resp, json!([{ "title": "Orphan" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// RefList поля — $some / $none / $every
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_ref_list_some() {
    let (db, _dir) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob" }));
    insert_data(&db, "Post", json!({ "title": "Alice post", "author": alice }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "posts": { "$some": {} } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn process_where_ref_list_none() {
    let (db, _dir) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob" }));
    insert_data(&db, "Post", json!({ "title": "Alice post", "author": alice }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "posts": { "$none": {} } }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

#[test]
fn process_where_ref_list_every() {
    let (db, _dir) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob"   }));

    insert_data(&db, "Post", json!({ "title": "Alice post1", "author": alice.clone() }));
    insert_data(&db, "Post", json!({ "title": "Alice post2", "author": alice }));
    insert_data(&db, "Post", json!({ "title": "Bob post",    "author": bob }));

    // Пользователи, у которых все посты содержат "Alice" в заголовке
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "posts": { "$every": { "title": { "$startsWith": "Alice" } } } }
    }));
    // Только Alice имеет все посты с "Alice" в начале
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Вложенный struct в where
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_nested_struct_field() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "info": { "bio": "dev", "level": 5 } }));
    insert_data(&db, "User", json!({ "name": "Bob",   "info": { "bio": "qa",  "level": 2 } }));
    insert_data(&db, "User", json!({ "name": "Charlie" }));

    // info не null и level > 3 → Alice
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "info": { "level": { "$gt": 3 } } }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn process_where_nested_struct_null() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "info": { "bio": "dev", "level": 1 } }));
    insert_data(&db, "User", json!({ "name": "Bob" }));

    // info == null → Bob
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "info": null }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Where::Or — несколько условий
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_or_conditions() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice",   "age": 20 }));
    insert_data(&db, "User", json!({ "name": "Bob",     "age": 30 }));
    insert_data(&db, "User", json!({ "name": "Charlie", "age": 40 }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": {
            "$or": [
                { "name": "Alice" },
                { "age": { "$gte": 40 } }
            ]
        }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Bob"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Where::Not
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_not_condition() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice",   "score": -10 }));
    insert_data(&db, "User", json!({ "name": "Bob",     "score": 5   }));
    insert_data(&db, "User", json!({ "name": "Charlie", "score": 0   }));

    // NOT(score < 0) → Bob + Charlie
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "$not": { "score": { "$lt": 0 } } }
    }));
    let arr = resp.as_array().unwrap();
    let names: Vec<&str> = arr.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Alice"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Boolean field comparisons
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_bool_true() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "active": true  }));
    insert_data(&db, "User", json!({ "name": "Bob",   "active": false }));
    insert_data(&db, "User", json!({ "name": "Carol", "active": null  }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "active": true }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}

#[test]
fn process_where_bool_false() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "active": true  }));
    insert_data(&db, "User", json!({ "name": "Bob",   "active": false }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "active": false }
    }));
    assert_eq!(resp, json!([{ "name": "Bob" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Проверка $startsWith + index
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_starts_with_no_match() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "User", json!({ "name": "Bob"   }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$startsWith": "Z" } }
    }));
    assert_eq!(resp, json!([]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Несколько условий в одном where (AND по умолчанию)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn process_where_implicit_and() {
    let (db, _dir) = make_db();
    insert_data(&db, "User", json!({ "name": "Alice", "age": 20, "active": true  }));
    insert_data(&db, "User", json!({ "name": "Bob",   "age": 20, "active": false }));
    insert_data(&db, "User", json!({ "name": "Carol", "age": 30, "active": true  }));

    // age == 20 AND active == true → Alice
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": 20, "active": true }
    }));
    assert_eq!(resp, json!([{ "name": "Alice" }]));
}
