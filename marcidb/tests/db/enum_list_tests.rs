// ─────────────────────────────────────────────────────────────────────────────
// Файл: tests/enum_list_tests.rs
//
// Покрывает непокрытые ветви в process_where.rs:
//   - FieldCompare::EnumListSome   ($some на поле struct[] с enum-полем)
//   - FieldCompare::EnumListEvery  ($every на поле struct[] с enum-полем)
//   - FieldCompare::EnumListNone   ($none на поле struct[] с enum-полем)
//
// Добавить в tests/mod.rs строку:
//   pub mod enum_list_tests;
// ─────────────────────────────────────────────────────────────────────────────

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

// Схема: модель Project содержит встроенный список структур ProjectUser,
// каждая из которых имеет enum-поле role.
fn make_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model Project {
            name    String
            users   ProjectUser[]
        }

        struct ProjectUser {
            user    User    @id
            role    Role
        }

        model User {
            name    String
        }

        enum Role {
            viewer
            editor
            owner
        }
    ";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice"   }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob"     }));
    let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));

    // Alpha: owner + editor
    insert_data(&db, "Project", json!({
        "name": "Alpha",
        "users": [
            { "user": user_a, "role": "owner"  },
            { "user": user_b, "role": "editor" }
        ]
    }));

    // Beta: только viewer
    insert_data(&db, "Project", json!({
        "name": "Beta",
        "users": [
            { "user": user_c, "role": "viewer" }
        ]
    }));

    // Gamma: owner + owner
    insert_data(&db, "Project", json!({
        "name": "Gamma",
        "users": [
            { "user": user_a, "role": "owner" },
            { "user": user_b, "role": "owner" }
        ]
    }));

    // Delta: пустой список участников
    insert_data(&db, "Project", json!({
        "name": "Delta"
    }));

    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// EnumListSome: хотя бы один участник с заданной ролью
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_some_test() {
    let (db, _dir) = make_db();

    // Проекты, в которых есть хотя бы один owner → Alpha и Gamma
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$some": { "role": "owner" } } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alpha" },
        { "name": "Gamma" }
    ]));
}

#[test]
fn enum_list_some_viewer_test() {
    let (db, _dir) = make_db();

    // Проекты, в которых есть хотя бы один viewer → только Beta
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$some": { "role": "viewer" } } }
    }));
    assert_eq!(resp, json!([ { "name": "Beta" } ]));
}

#[test]
fn enum_list_some_empty_list_test() {
    let (db, _dir) = make_db();

    // $some на пустом списке → false, Delta не попадает
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$some": { "role": "owner" } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(!names.contains(&"Delta"));
}

// ─────────────────────────────────────────────────────────────────────────────
// EnumListEvery: каждый участник имеет заданную роль
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_every_test() {
    let (db, _dir) = make_db();

    // Проекты, где каждый участник — owner:
    //   Alpha: owner + editor → нет
    //   Beta:  viewer         → нет
    //   Gamma: owner + owner  → да
    //   Delta: пустой список  → вакуумная истина → да
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$every": { "role": "owner" } } }
    }));
    assert_eq!(resp, json!([
        { "name": "Gamma" },
        { "name": "Delta" }
    ]));
}

#[test]
fn enum_list_every_empty_is_vacuously_true_test() {
    let (db, _dir) = make_db();

    // Пустой список всегда проходит $every (вакуумная истина)
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$every": { "role": "viewer" } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Delta"));
}

// ─────────────────────────────────────────────────────────────────────────────
// EnumListNone: ни одного участника с заданной ролью
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_none_test() {
    let (db, _dir) = make_db();

    // Проекты без ни одного viewer:
    //   Alpha: owner + editor → нет viewer → да
    //   Beta:  viewer         → есть viewer → нет
    //   Gamma: owner + owner  → нет viewer → да
    //   Delta: пустой список  → нет viewer → да
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$none": { "role": "viewer" } } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alpha" },
        { "name": "Gamma" },
        { "name": "Delta" }
    ]));
}

#[test]
fn enum_list_none_empty_list_test() {
    let (db, _dir) = make_db();

    // $none на пустом списке → true, Delta всегда проходит
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$none": { "role": "owner" } } }
    }));
    let names: Vec<_> = resp.as_array().unwrap()
        .iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Delta"));
}
