//! What a refused write says. The message is for a person — the field, the id as the client wrote it,
//! what to do —, `kind()` is what a client switches on, `field()` the `Model.field` it is about.
//! Clients see exactly these (the FFI envelope, the server's JSON body), so the texts are pinned here.
use marcidb::{DeleteError, InsertError, MarciDB, UpdateError, execute_batch, parse_insert};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{insert_data, try_delete, try_insert, try_update};

const SCHEMA: &str = "
    model User {
        email   String  @unique
        name    String
    }

    model Dish {
        id      String  @id
        name    String
    }

    model Order {
        dish    Dish
        qty     Int
    }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

#[test]
fn a_refused_write_names_the_field_and_the_id() {
    let (_dir, db) = make_db();
    insert_data(&db, "User", json!({ "email": "a@x.io", "name": "Alice" }));
    insert_data(&db, "Dish", json!({ "id": "latte", "name": "Latte" }));
    let order = insert_data(&db, "Order", json!({ "dish": { "id": "latte" }, "qty": 1 }));

    let e = try_insert(&db, "User", json!({ "email": "a@x.io", "name": "Al" })).unwrap_err();
    assert_eq!(e.to_string(), "User.email: the value is already taken (by the row with id 1)");
    assert_eq!((e.kind(), e.field()), ("unique", Some("User.email")));

    let e = try_insert(&db, "Order", json!({ "dish": { "id": "nothing" }, "qty": 1 })).unwrap_err();
    assert_eq!(e.to_string(), "Order.dish: no row with id \"nothing\" to point at");
    assert_eq!((e.kind(), e.field()), ("foreign_key", Some("Order.dish")));

    let e = try_insert(&db, "Dish", json!({ "id": "latte", "name": "Again" })).unwrap_err();
    assert_eq!(e.to_string(), "Dish: a row with id \"latte\" already exists");
    assert_eq!((e.kind(), e.field()), ("duplicate_id", None));
    assert!(matches!(e, InsertError::IdViolation(..)));

    let e = try_update(&db, "Order", &order, json!({ "dish": { "$connect": { "id": "nothing" } } })).unwrap_err();
    assert_eq!(e.to_string(), "Order.dish: no row with id \"nothing\" to point at");
    assert_eq!((e.kind(), e.field()), ("foreign_key", Some("Order.dish")));
    assert!(matches!(e, UpdateError::ForeignKeyViolation(..)));

    let e = try_delete(&db, "Dish", json!({ "id": "latte" })).unwrap_err();
    assert_eq!(e.to_string(), "Order.dish: rows 1 still point at this one; delete or repoint them first");
    assert_eq!((e.kind(), e.field()), ("restrict", Some("Order.dish")));
    assert!(matches!(e, DeleteError::RestrictConstraints(..)));
}

#[test]
fn a_bad_payload_says_what_is_wrong() {
    let (_dir, db) = make_db();
    let user = db.get_model("User").unwrap();
    let e = parse_insert(&db.schema, user, &json!({ "email": "b@x.io" })).unwrap_err();
    assert_eq!(e.to_string(), "User.name is required");
    let e = parse_insert(&db.schema, user, &json!({ "email": "b@x.io", "name": 5 })).unwrap_err();
    assert_eq!(e.to_string(), "User.name: expected string");
}

#[test]
fn a_batch_names_the_operation_and_keeps_the_kind() {
    let (_dir, db) = make_db();
    insert_data(&db, "Dish", json!({ "id": "latte", "name": "Latte" }));
    let e = execute_batch(&db, &[
        json!({ "model": "Order", "action": "insert", "data": { "dish": { "id": "latte" }, "qty": 1 } }),
        json!({ "model": "Order", "action": "insert", "data": { "dish": { "id": "nothing" }, "qty": 1 } }),
    ]).unwrap_err();
    assert_eq!(e.to_string(), "batch op #1: Order.dish: no row with id \"nothing\" to point at");
    assert_eq!((e.kind(), e.field()), ("foreign_key", Some("Order.dish")));
    assert_eq!(db.count(db.get_model("Order").unwrap()).unwrap(), 0, "the batch rolled back");
}
