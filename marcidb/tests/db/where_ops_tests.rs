use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

// ─────────────────────────────────────────────────────────────────────────────
// Вспомогательная схема, используемая в большинстве тестов этого модуля
// ─────────────────────────────────────────────────────────────────────────────
fn make_user_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model User {
            name        String
            age         UInt?
            email       String?
            score       Int?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice",   "age": 20, "email": "alice@test.com",   "score": -5  }));
    insert_data(&db, "User", json!({ "name": "Bob",     "age": 40, "email": "bob@test.com",     "score": 10  }));
    insert_data(&db, "User", json!({ "name": "Charlie", "age": 18, "email": null,               "score": 5   }));
    insert_data(&db, "User", json!({ "name": "Unknown", "age": null, "email": null,             "score": null }));

    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// $lt / $lte
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_lt_test() {
    let (db, _dir) = make_user_db();

    // $lt: строго меньше 20 → только Charlie (18)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$lt": 20 } }
    }));
    assert_eq!(resp, json!([ { "name": "Charlie" } ]));
}

#[test]
fn where_lte_test() {
    let (db, _dir) = make_user_db();

    // $lte: меньше или равно 20 → Alice (20) и Charlie (18)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$lte": 20 } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Charlie" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $gte
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_gte_test() {
    let (db, _dir) = make_user_db();

    // $gte: больше или равно 20 → Alice (20) и Bob (40)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$gte": 20 } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Bob" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $eq (явный оператор равенства)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_eq_test() {
    let (db, _dir) = make_user_db();

    // $eq на строке — эквивалентен точному значению
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "name": { "$eq": "Alice" } }
    }));
    assert_eq!(resp, json!([ { "name": "Alice" } ]));

    // $eq: null — эквивалентен проверке на отсутствие значения
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$eq": null } }
    }));
    assert_eq!(resp, json!([ { "name": "Unknown" } ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $ne (не равно конкретному значению; null-значения проходят)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_ne_test() {
    let (db, _dir) = make_user_db();

    // $ne: 20 → Bob, Charlie, Unknown (null тоже проходит — поле отсутствует)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$ne": 20 } }
    }));
    assert_eq!(resp, json!([
        { "name": "Bob" },
        { "name": "Charlie" },
        { "name": "Unknown" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $in / $notIn
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_in_test() {
    let (db, _dir) = make_user_db();

    // $in: конкретные значения → Alice (20) и Bob (40)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$in": [20, 40] } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Bob" }
    ]));

    // $in с null в списке → Charlie (null email) и Unknown (null email) + Alice
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$in": ["alice@test.com", null] } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Charlie" },
        { "name": "Unknown" }
    ]));
}

#[test]
fn where_not_in_test() {
    let (db, _dir) = make_user_db();

    // $notIn: исключаем Alice и Bob → Charlie и Unknown
    // null-поля проходят, т.к. null не входит в список
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$notIn": [20, 40] } }
    }));
    assert_eq!(resp, json!([
        { "name": "Charlie" },
        { "name": "Unknown" }
    ]));

    // $notIn с null: исключаем null-значения и конкретное значение
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": { "email": { "$notIn": ["bob@test.com", null] } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $and (логический AND верхнего уровня через ключ $and)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_and_test() {
    let (db, _dir) = make_user_db();

    // $and: age >= 18 AND age <= 20 → Alice (20) и Charlie (18)
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": {
            "$and": [
                { "age": { "$gte": 18 } },
                { "age": { "$lte": 20 } }
            ]
        }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Charlie" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $not (отрицание всего условия верхнего уровня)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_not_test() {
    let (db, _dir) = make_user_db();

    // $not { age: { $lt: 30 } } → исключаем тех, у кого age < 30
    // Alice (20) и Charlie (18) исключаются; Bob (40) и Unknown (null) проходят
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": {
            "$not": { "age": { "$lt": 30 } }
        }
    }));
    assert_eq!(resp, json!([
        { "name": "Bob" },
        { "name": "Unknown" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Комбинированный тест: несколько операторов сравнения в одном запросе
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_combined_ops_test() {
    let (db, _dir) = make_user_db();

    // age >= 18 AND age < 40 AND email != null → Alice (20) и Charlie (18, null email исключён)
    // Но у Charlie email = null, поэтому в итоге только Alice
    let resp = get_data(&db, "User", json!({
        "name": true,
        "$where": {
            "age": { "$gte": 18 },
            "email": { "$not": null }
        }
    }));
    assert_eq!(resp, json!([
        { "name": "Alice" },
        { "name": "Bob" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $every на RefList
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_every_ref_list_test() {
    let schema_str = "
        model User {
            name    String
            chats   Chat[]
        }

        model Chat {
            name    String
            users   User[]  @bind(User.chats)
        }
    ";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_alice   = insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_bob     = insert_data(&db, "User", json!({ "name": "Bob"   }));
    let user_charlie = insert_data(&db, "User", json!({ "name": "Charlie" }));

    // Пустой чат — $every должен давать true (вакуумная истина)
    insert_data(&db, "Chat", json!({ "name": "Empty",   "users": [] }));
    // Чат только с Alice
    insert_data(&db, "Chat", json!({ "name": "Solo",    "users": [ user_alice ] }));
    // Чат с Alice и Bob
    insert_data(&db, "Chat", json!({ "name": "AliceBob", "users": [ user_alice, user_bob ] }));
    // Чат со всеми
    insert_data(&db, "Chat", json!({ "name": "All",     "users": [ user_alice, user_bob, user_charlie ] }));

    // Найдём чаты, где каждый участник — Alice
    // Пустой чат: вакуумная истина → подходит
    // Solo: только Alice → подходит
    // AliceBob: Bob не Alice → не подходит
    // All: Charlie и Bob не Alice → не подходит
    let resp = get_data(&db, "Chat", json!({
        "name": true,
        "$where": { "users": { "$every": { "name": "Alice" } } }
    }));
    assert_eq!(resp, json!([
        { "name": "Empty" },
        { "name": "Solo" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Фильтрация по enum-полю
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn where_enum_field_test() {
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

    let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob"   }));
    let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));

    insert_data(&db, "Project", json!({
        "name": "Alpha",
        "users": [
            { "user": user_a, "role": "owner"  },
            { "user": user_b, "role": "editor" }
        ]
    }));
    insert_data(&db, "Project", json!({
        "name": "Beta",
        "users": [
            { "user": user_c, "role": "viewer" }
        ]
    }));
    insert_data(&db, "Project", json!({
        "name": "Empty"
    }));

    // Проекты, в которых есть хотя бы один owner
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$some": { "role": "owner" } } }
    }));
    assert_eq!(resp, json!([ { "name": "Alpha" } ]));

    // Проекты, в которых нет ни одного viewer
    let resp = get_data(&db, "Project", json!({
        "name": true,
        "$where": { "users": { "$none": { "role": "viewer" } } }
    }));
    assert_eq!(resp, json!([
        { "name": "Alpha" },
        { "name": "Empty" }
    ]));
}

// ─────────────────────────────────────────────────────────────────────────────
// find_first при отсутствии результата возвращает null
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn find_first_empty_result_test() {
    let (db, _dir) = make_user_db();

    let result = get_data_one(&db, "User", json!({
        "name": true,
        "$where": { "age": { "$gt": 9999 } }
    }));

    assert_eq!(result, serde_json::Value::Null);
}
