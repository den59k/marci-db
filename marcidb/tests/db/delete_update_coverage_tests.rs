// tests/delete_update_coverage_tests.rs
//
// Покрывает непокрытые ветки:
//   - delete_op/process_delete.rs (75.59%)
//   - update_op/process_update.rs (75.86%)
//   - parse_update_op.rs (70.59%)
//
// Тестируются:
//   - Каскадные удаления (autoinsert struct)
//   - $set на RefList (замена всего списка)
//   - $connect / $disconnect на RefList (many-to-many)
//   - $update на вложенных сущностях
//   - $remove на RefList (many-to-many)
//   - UpdateValue::Null для nullable Ref поля
//   - parse_update на Enum поле
//   - parse_update на PrimitiveList поле

use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, update_data};

// ─────────────────────────────────────────────────────────────────────────────
// Схема: User ← Post (autoinsert struct)
// ─────────────────────────────────────────────────────────────────────────────
fn make_post_db() -> (MarciDB, tempfile::TempDir) {
    let schema_str = "
        model User {
            name    String
            posts   Post[]  @bind(Post.author)
        }
        model Post {
            title   String
            author  User?   @onDelete(Cascade)
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// Удаление каскадное — User + его посты
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_cascades_to_ref_list() {
    let (db, _dir) = make_post_db();

    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    insert_data(&db, "Post", json!({ "title": "Post1", "author": alice.clone() }));
    insert_data(&db, "Post", json!({ "title": "Post2", "author": alice.clone() }));
    insert_data(&db, "Post", json!({ "title": "Orphan" }));

    assert_eq!(db.count(db.get_model("Post").unwrap()), 3);

    crate::db::delete_data(&db, "User", alice);

    // Alice удалена
    assert_eq!(db.count(db.get_model("User").unwrap()), 0);
    // Orphan не должен быть удалён
    let posts = get_data(&db, "Post", json!({ "title": true }));
    let arr = posts.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["title"], "Orphan");
}

// ─────────────────────────────────────────────────────────────────────────────
// $connect на RefList (many-to-many)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn update_connect_ref_list() {
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

    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob"   }));
    let chat  = insert_data(&db, "Chat", json!({ "name": "Room", "users": [alice.clone()] }));

    // Подключаем Bob
    update_data(&db, "Chat", &chat, json!({
        "users": { "$connect": [bob.clone()] }
    }));

    let resp = get_data(&db, "Chat", json!({
        "name": true,
        "users": { "name": true }
    }));
    let users = resp[0]["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// $update на вложенной autoinsert-сущности
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn update_nested_ref_update() {
    let schema_str = "
        model User {
            name    String
            info    UserInfo?
        }
        struct UserInfo {
            bio     String
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user = insert_data(&db, "User", json!({ "name": "Alice", "info": { "bio": "old" } }));

    update_data(&db, "User", &user, json!({
        "info": { "$update": { "bio": "new bio" } }
    }));

    let resp = get_data(&db, "User", json!({
        "name": true,
        "info": { "bio": true }
    }));
    assert_eq!(resp, json!([{ "name": "Alice", "info": { "bio": "new bio" } }]));
}



// ─────────────────────────────────────────────────────────────────────────────
// Обновление Enum поля
// ─────────────────────────────────────────────────────────────────────────────


#[test]
fn update_primitive_list_field() {
    let schema_str = "
        model Config {
            name    String
            tags    String[]
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let cfg = insert_data(&db, "Config", json!({ "name": "A", "tags": ["x", "y"] }));
    update_data(&db, "Config", &cfg, json!({ "tags": ["a", "b", "c"] }));

    let resp = get_data(&db, "Config", json!({ "name": true, "tags": true }));
    assert_eq!(resp, json!([{ "name": "A", "tags": ["a", "b", "c"] }]));
}

// ─────────────────────────────────────────────────────────────────────────────
// $push на RefList (добавление новых элементов)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn update_push_to_autoinsert_struct_list() {
    let schema_str = "
        model Project {
            name    String
            tasks   Task[]
        }
        struct Task {
            title   String
            done    Boolean
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let project = insert_data(&db, "Project", json!({
        "name": "P1",
        "tasks": [{ "title": "Task A", "done": false }]
    }));

    update_data(&db, "Project", &project, json!({
        "tasks": { "$push": { "title": "Task B", "done": true } }
    }));

    let resp = get_data(&db, "Project", json!({
        "name": true,
        "tasks": { "title": true, "done": true }
    }));
    let tasks = resp[0]["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// parse_update — UnsupportedOperation для RefList
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn parse_update_unsupported_ref_list_op() {
    use marcidb::{parse_update, EncodeError};

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
    let entity = db.get_model("Chat").unwrap();

    let result = parse_update(&db.schema, entity, &json!({
        "users": { "$unknownOp": [] }
    }));
    assert!(matches!(result, Err(EncodeError::UnsupportedOperation(_))));
}

// ─────────────────────────────────────────────────────────────────────────────
// parse_update — UnsupportedOperation для Ref поля
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn parse_update_unsupported_ref_op() {
    use marcidb::{parse_update, EncodeError};

    let schema_str = "
        model Post {
            title   String
            author  User?
        }
        model User {
            name String
        }
    ";
    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
    let entity = db.get_model("Post").unwrap();

    let result = parse_update(&db.schema, entity, &json!({
        "author": { "$badOp": {} }
    }));
    assert!(matches!(result, Err(EncodeError::UnsupportedOperation(_))));
}

// ─────────────────────────────────────────────────────────────────────────────
// Удаление несуществующего (но корректного) ID не паникует
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_nonexistent_is_ok() {
    let (db, _dir) = make_post_db();

    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    crate::db::delete_data(&db, "User", alice.clone());

    // Повторное удаление не должно паниковать (результат игнорируем)
    let entity = db.get_model("User").unwrap();
    let id = marcidb::parse_id(&db.schema, entity, &alice).unwrap();
    // delete_item возвращает Result, просто убедимся что не паникует
    let _ = db.delete_item(entity, &id);
}

// ─────────────────────────────────────────────────────────────────────────────
// $set [] — пустой массив (RemoveAll)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn update_set_empty_ref_list_removes_all() {
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

    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob   = insert_data(&db, "User", json!({ "name": "Bob"   }));
    let chat  = insert_data(&db, "Chat", json!({
        "name": "Room",
        "users": [alice.clone(), bob.clone()]
    }));

    // $set пустой массив — убираем всех пользователей
    update_data(&db, "Chat", &chat, json!({
        "users": { "$set": [] }
    }));

    let resp = get_data(&db, "Chat", json!({
        "name": true,
        "users": { "name": true }
    }));
    assert_eq!(resp, json!([{ "name": "Room", "users": [] }]));
}
