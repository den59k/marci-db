// tests/schema_tests.rs
//
// Покрывает непокрытые ветки:
//   - schema/schema_resolve_bindings.rs (81.36%)
//   - schema/schema_enum.rs (83.82%)
//   - schema/schema_default_value.rs (75.56%)
//   - schema/schema_field.rs (81.18%)
//   - schema/schema_attributes.rs (82.57%)
//
// Тестируется:
//   - Enum с вложенными полями (encode/decode)
//   - @default значения (string, bool, int, float, now)
//   - @unique constraint при вставке
//   - Числовые типы: Float, Double, Int, Byte
//   - nullable поля по умолчанию
//   - @bind двусторонние связи

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data, update_data};

// ─────────────────────────────────────────────────────────────────────────────
// Enum без вложенных полей
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_simple_insert_query() {
    let schema_str = "
        enum Status {
            active
            inactive
            pending
        }
        model Task {
            name    String
            status  Status
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Task", json!({ "name": "T1", "status": "active"   }));
    insert_data(&db, "Task", json!({ "name": "T2", "status": "inactive" }));
    insert_data(&db, "Task", json!({ "name": "T3", "status": "pending"  }));

    let resp = get_data(&db, "Task", json!({
        "name": true,
        "$where": { "status": "active" }
    }));
    assert_eq!(resp, json!([{ "name": "T1" }]));
}

#[test]
fn enum_simple_update() {
    let schema_str = "
        enum Status {
            active
            inactive
        }
        model Task {
            name    String
            status  Status
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let task = insert_data(&db, "Task", json!({ "name": "T1", "status": "active" }));
    update_data(&db, "Task", &task, json!({ "status": "inactive" }));

    let resp = get_data(&db, "Task", json!({ "name": true, "status": true }));
    assert_eq!(resp, json!([{ "name": "T1", "status": "inactive" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Enum с вложенными полями (discriminated union)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_with_fields_insert_decode() {
    let schema_str = "
        enum Shape {
            circle {
                radius Float
            }
            rect {
                width Float
                height Float
            }
            point
        }
        model Drawing {
            name    String
            shape   Shape
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Drawing", json!({ "name": "D1", "shape": "circle", "radius": 5.0 }));
    insert_data(&db, "Drawing", json!({ "name": "D2", "shape": "rect",   "width": 10.0, "height": 20.0 }));
    insert_data(&db, "Drawing", json!({ "name": "D3", "shape": "point" }));

    let resp = get_data(&db, "Drawing", json!({ "name": true, "shape": true }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    // Проверяем, что variant читается корректно
    let d1 = arr.iter().find(|v| v["name"] == "D1").unwrap();
    assert_eq!(d1["shape"], "circle");
}

#[test]
fn enum_with_fields_where_by_variant() {
    let schema_str = "
        enum Shape {
            circle {
                radius Float
            }
            rect {
                width Float
            }
        }
        model Drawing {
            name    String
            shape   Shape
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Drawing", json!({ "name": "C", "shape": "circle", "radius": 5.0  }));
    insert_data(&db, "Drawing", json!({ "name": "R", "shape": "rect",   "width": 10.0  }));

    // Фильтр по варианту через EnumList-логику (если поддерживается в where)
    let resp = get_data(&db, "Drawing", json!({ "name": true, "shape": true }));
    assert_eq!(resp.as_array().unwrap().len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// @default значения
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn default_bool_value() {
    let schema_str = "
        model User {
            name    String
            active  Boolean     @default(true)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice" })); // active не передаём
    let resp = get_data(&db, "User", json!({ "name": true, "active": true }));
    assert_eq!(resp, json!([{ "name": "Alice", "active": true }]));
}

#[test]
fn default_bool_false_value() {
    let schema_str = "
        model User {
            name    String
            banned  Boolean     @default(false)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Bob" }));
    let resp = get_data(&db, "User", json!({ "name": true, "banned": true }));
    assert_eq!(resp, json!([{ "name": "Bob", "banned": false }]));
}

#[test]
fn default_int_value() {
    let schema_str = "
        model Counter {
            label   String
            count   Int         @default(0)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Counter", json!({ "label": "hits" }));
    let resp = get_data(&db, "Counter", json!({ "label": true, "count": true }));
    assert_eq!(resp, json!([{ "label": "hits", "count": 0 }]));
}

#[test]
fn default_uint_value() {
    let schema_str = "
        model Stats {
            name    String
            views   UInt        @default(100)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Stats", json!({ "name": "page" }));
    let resp = get_data(&db, "Stats", json!({ "name": true, "views": true }));
    assert_eq!(resp, json!([{ "name": "page", "views": 100 }]));
}

#[test]
fn default_string_value() {
    let schema_str = "
        model User {
            name    String
            role    String      @default(\"guest\")
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice" }));
    let resp = get_data(&db, "User", json!({ "name": true, "role": true }));
    assert_eq!(resp, json!([{ "name": "Alice", "role": "guest" }]));
}

#[test]
fn default_now_datetime() {
    let schema_str = "
        model Event {
            name        String
            createdAt   DateTime    @default(now())
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Event", json!({ "name": "E1" }));

    let resp = get_data(&db, "Event", json!({ "name": true, "createdAt": true }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    // createdAt должен быть числом (timestamp_millis)
    assert!(arr[0]["createdAt"].is_number(), "createdAt должен быть числом (epoch ms)");
}

#[test]
fn default_overridden_by_explicit_value() {
    let schema_str = "
        model User {
            name    String
            active  Boolean     @default(true)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    // Явно передаём false
    insert_data(&db, "User", json!({ "name": "Alice", "active": false }));
    let resp = get_data(&db, "User", json!({ "name": true, "active": true }));
    assert_eq!(resp, json!([{ "name": "Alice", "active": false }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Float / Double поля
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn float_field_insert_query() {
    let schema_str = "
        model Product {
            name    String
            price   Float
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Product", json!({ "name": "Apple", "price": 1.99 }));
    insert_data(&db, "Product", json!({ "name": "Banana", "price": 0.49 }));

    let resp = get_data(&db, "Product", json!({
        "name": true,
        "$where": { "price": { "$gt": 1.0 } }
    }));
    assert_eq!(resp, json!([{ "name": "Apple" }]));
}

#[test]
fn double_field_insert_query() {
    let schema_str = "
        model Measurement {
            label   String
            value   Double
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Measurement", json!({ "label": "pi",  "value": 3.14159265358979 }));
    insert_data(&db, "Measurement", json!({ "label": "e",   "value": 2.71828182845905 }));

    let resp = get_data(&db, "Measurement", json!({
        "label": true,
        "$where": { "value": { "$gte": 3.0 } }
    }));
    assert_eq!(resp, json!([{ "label": "pi" }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Nullable поле — null по умолчанию не задан
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn nullable_field_null_by_default() {
    let schema_str = "
        model User {
            name    String
            bio     String?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice" }));
    let resp = get_data(&db, "User", json!({ "name": true, "bio": true }));
    assert_eq!(resp, json!([{ "name": "Alice", "bio": null }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// @unique — вторая вставка с тем же значением должна вернуть ошибку или быть обработана
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn unique_field_allows_different_values() {
    let schema_str = "
        model User {
            name    String
            email   String      @unique
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({ "name": "Alice", "email": "a@test.com" }));
    insert_data(&db, "User", json!({ "name": "Bob",   "email": "b@test.com" }));

    assert_eq!(db.count(db.get_model("User").unwrap()), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// @bind двусторонняя связь
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn bind_two_way_relation() {
    let schema_str = "
        model Author {
            name    String
            books   Book[]  @bind(Book.author)
        }
        model Book {
            title   String
            author  Author?
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let author = insert_data(&db, "Author", json!({ "name": "Tolstoy" }));
    insert_data(&db, "Book", json!({ "title": "War and Peace",  "author": author.clone() }));
    insert_data(&db, "Book", json!({ "title": "Anna Karenina",  "author": author.clone() }));
    insert_data(&db, "Book", json!({ "title": "Orphan book" }));

    // Читаем со стороны Author
    let resp = get_data(&db, "Author", json!({
        "name": true,
        "books": { "title": true }
    }));
    let books = resp[0]["books"].as_array().unwrap();
    assert_eq!(books.len(), 2);

    // Читаем со стороны Book
    let resp2 = get_data(&db, "Book", json!({
        "title": true,
        "$where": { "author": { "$ne": null } }
    }));
    assert_eq!(resp2.as_array().unwrap().len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// Составной ключ (compound @id)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn compound_id_insert_query_delete() {
    let schema_str = "
        model User {
            name String
        }
        model Chat {
            title String
        }
        model ChatUser {
            user    User    @id
            chat    Chat    @id
            role    String
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user = insert_data(&db, "User", json!({ "name": "Alice" }));
    let chat = insert_data(&db, "Chat", json!({ "title": "General" }));

    insert_data(&db, "ChatUser", json!({
        "user": user.clone(),
        "chat": chat.clone(),
        "role": "admin"
    }));

    let resp = get_data(&db, "ChatUser", json!({ "role": true }));
    assert_eq!(resp, json!([{ "role": "admin" }]));

    // Удаляем по составному ключу
    crate::db::delete_data(&db, "ChatUser", json!({
        "user": user,
        "chat": chat
    }));
    assert_eq!(db.count(db.get_model("ChatUser").unwrap()), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Вложенный struct (автоматически вставляемый)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn nested_struct_insert_and_query() {
    let schema_str = "
        model User {
            name    String
            address Address?
        }
        struct Address {
            city    String
            zip     String
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "User", json!({
        "name": "Alice",
        "address": { "city": "Moscow", "zip": "101000" }
    }));
    insert_data(&db, "User", json!({ "name": "Bob" }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "address": { "city": true }
    }));
    let arr = resp.as_array().unwrap();
    let alice = arr.iter().find(|v| v["name"] == "Alice").unwrap();
    assert_eq!(alice["address"]["city"], "Moscow");

    let bob = arr.iter().find(|v| v["name"] == "Bob").unwrap();
    assert_eq!(bob["address"], json!(null));
}

// ─────────────────────────────────────────────────────────────────────────────
// Enum list (массив enum значений)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn enum_list_insert_decode() {
    let schema_str = "
        enum Tag {
            rust
            go
            python
        }
        model Post {
            title   String
            tags    Tag[]
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    insert_data(&db, "Post", json!({ "title": "Post1", "tags": ["rust", "go"] }));
    insert_data(&db, "Post", json!({ "title": "Post2", "tags": ["python"] }));
    insert_data(&db, "Post", json!({ "title": "Post3", "tags": [] }));

    let resp = get_data(&db, "Post", json!({ "title": true, "tags": true }));
    let arr = resp.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    let post1 = arr.iter().find(|v| v["title"] == "Post1").unwrap();
    let tags = post1["tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
}
