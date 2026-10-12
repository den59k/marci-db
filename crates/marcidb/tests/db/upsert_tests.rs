//! `upsert(key, data)`: the row the key names is updated when it exists, inserted when not — by the id, by a
//! `@unique` field, by all the fields of a `@unique(<name>)` group. The data is in the insert form and is
//! translated for the update; operators are refused; `onInsert` is for the creation alone.
use marcidb::{MarciDB, UpsertError, execute_batch};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, try_upsert, upsert_data};

const SCHEMA: &str = "
    model User {
        email   String  @unique
        name    String
        tags    String[]
        home    Address?
    }

    struct Address {
        city    String
    }

    model Setting {
        user    User    @unique(pref)
        key     String  @unique(pref)
        value   String
        changes Int     @default(0)
    }

    model Dish {
        id      String  @id
        name    String
        price   Int
    }

    model Post {
        slug    String  @unique
        title   String
        author  User?
        tags    Tag[]
    }

    model Tag {
        name    String
    }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

#[test]
fn by_a_unique_group_the_row_is_created_once_and_then_updated() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "email": "a@x.io", "name": "Alice" }));
    let bob = insert_data(&db, "User", json!({ "email": "b@x.io", "name": "Bob" }));

    let first = upsert_data(&db, "Setting", json!({ "user": alice.clone(), "key": "theme" }), json!({ "value": "dark" }));
    let again = upsert_data(&db, "Setting", json!({ "user": alice.clone(), "key": "theme" }), json!({ "value": "light" }));
    assert_eq!(first, again, "the same row");
    upsert_data(&db, "Setting", json!({ "user": alice.clone(), "key": "lang" }), json!({ "value": "ru" }));
    upsert_data(&db, "Setting", json!({ "user": bob.clone(), "key": "theme" }), json!({ "value": "dark" }));
    assert_eq!(
        get_data(&db, "Setting", json!({ "key": true, "value": true, "user": { "name": true }, "$order": { "id": "asc" } })),
        json!([
            { "key": "theme", "value": "light", "user": { "name": "Alice" } },
            { "key": "lang", "value": "ru", "user": { "name": "Alice" } },
            { "key": "theme", "value": "dark", "user": { "name": "Bob" } },
        ])
    );
}

#[test]
fn by_the_id_and_by_a_unique_field() {
    let (_dir, db) = make_db();
    // the id, declared by the model: an insert with that id, then an update of it
    upsert_data(&db, "Dish", json!({ "id": "latte" }), json!({ "name": "Latte", "price": 290 }));
    upsert_data(&db, "Dish", json!({ "id": "latte" }), json!({ "price": 310 }));
    assert_eq!(get_data(&db, "Dish", json!({})), json!([{ "id": "latte", "name": "Latte", "price": 310 }]));

    // a @unique field
    let u = upsert_data(&db, "User", json!({ "email": "a@x.io" }), json!({ "name": "Alice", "tags": ["x"] }));
    let u2 = upsert_data(&db, "User", json!({ "email": "a@x.io" }), json!({ "name": "Alicia" }));
    assert_eq!(u, u2);
    assert_eq!(get_data(&db, "User", json!({ "name": true, "tags": true })), json!([{ "name": "Alicia", "tags": ["x"] }]));

    // the data in the insert form on the update path: a struct, a relation { id }, a list of relations
    let tag = insert_data(&db, "Tag", json!({ "name": "rust" }));
    upsert_data(&db, "Post", json!({ "slug": "hello" }), json!({ "title": "Hello" }));
    upsert_data(&db, "Post", json!({ "slug": "hello" }), json!({ "title": "Hello!", "author": u.clone(), "tags": [ tag.clone() ] }));
    upsert_data(&db, "User", json!({ "email": "a@x.io" }), json!({ "home": { "city": "Riga" } }));
    assert_eq!(
        get_data(&db, "Post", json!({ "title": true, "author": { "name": true, "home": { "city": true } }, "tags": { "name": true } })),
        json!([{ "title": "Hello!", "author": { "name": "Alicia", "home": { "city": "Riga" } }, "tags": [{ "name": "rust" }] }])
    );
    assert_eq!(get_data(&db, "Post", json!({})).as_array().unwrap().len(), 1);
}

#[test]
fn on_insert_is_for_the_creation_alone_and_the_key_wins() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "email": "a@x.io", "name": "Alice" }));
    let key = json!({ "user": alice.clone(), "key": "theme" });
    try_upsert(&db, "Setting", key.clone(), json!({ "value": "dark" }), Some(json!({ "changes": 7, "key": "ignored" }))).unwrap();
    try_upsert(&db, "Setting", key.clone(), json!({ "value": "light" }), Some(json!({ "changes": 99 }))).unwrap();
    assert_eq!(get_data(&db, "Setting", json!({ "key": true, "value": true, "changes": true })), json!([{ "key": "theme", "value": "light", "changes": 7 }]));
}

#[test]
fn what_is_refused() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "email": "a@x.io", "name": "Alice" }));

    // the key must name one row
    let e = try_upsert(&db, "Setting", json!({ "key": "theme" }), json!({ "value": "x" }), None).unwrap_err();
    assert!(matches!(e, UpsertError::BadRequest(_)), "{:?}", e);
    assert_eq!(e.to_string(), "Setting: the upsert key must be the id (id) or the fields of one unique index — a @unique field, or all the fields of a @unique(<name>) group; got key");
    assert_eq!(e.kind(), "bad_request");
    let e = try_upsert(&db, "User", json!({ "name": "Alice" }), json!({}), None).unwrap_err();
    assert!(matches!(e, UpsertError::BadRequest(_)));
    let e = try_upsert(&db, "Setting", json!({ "user": alice.clone(), "key": null }), json!({ "value": "x" }), None).unwrap_err();
    assert_eq!(e.to_string(), "Setting.key: a value of the upsert key cannot be null");

    // no operators: a row may be created by them
    let e = try_upsert(&db, "Setting", json!({ "user": alice.clone(), "key": "n" }), json!({ "value": "x", "changes": { "$increment": 1 } }), None).unwrap_err();
    assert_eq!(e.to_string(), "Setting.changes: upsert data takes the insert form — plain values, { id } for a relation; $increment is not allowed (a row may be created by it)");
    assert_eq!(get_data(&db, "Setting", json!({})), json!([]));

    // a missing required field on the creation: the insert's own error
    let e = try_upsert(&db, "Setting", json!({ "user": alice.clone(), "key": "n" }), json!({}), None).unwrap_err();
    assert_eq!(e.to_string(), "Setting.value is required");
    assert!(matches!(e, UpsertError::Parse(_)));
}

#[test]
fn in_a_batch_with_a_ref() {
    let (_dir, db) = make_db();
    let results = execute_batch(&db, &[
        json!({ "model": "User", "action": "insert", "data": { "email": "c@x.io", "name": "Carol" } }),
        json!({ "model": "Setting", "action": "upsert", "key": { "user": { "id": { "$ref": "0.id" } }, "key": "theme" }, "data": { "value": "dark" } }),
        json!({ "model": "Setting", "action": "upsert", "key": { "user": { "id": { "$ref": "0.id" } }, "key": "theme" }, "data": { "value": "light" }, "onInsert": { "changes": 5 } }),
    ]).unwrap();
    assert_eq!(results[1], results[2]);
    assert_eq!(get_data(&db, "Setting", json!({ "value": true, "changes": true })), json!([{ "value": "light", "changes": 0 }]));

    // the refusal names the operation and keeps the kind
    let e = execute_batch(&db, &[
        json!({ "model": "Setting", "action": "upsert", "key": { "key": "theme" }, "data": { "value": "dark" } }),
    ]).unwrap_err();
    assert!(e.to_string().starts_with("batch op #0: Setting: the upsert key must be"), "{}", e);
    assert_eq!(e.kind(), "bad_request");

    // update and delete of a row that is not there, in a batch: not_found, the batch rolls back
    let e = execute_batch(&db, &[
        json!({ "model": "User", "action": "insert", "data": { "email": "d@x.io", "name": "Dan" } }),
        json!({ "model": "User", "action": "delete", "id": { "id": 999 } }),
    ]).unwrap_err();
    assert_eq!((e.to_string().as_str(), e.kind()), ("batch op #1: the row does not exist", "not_found"));
    assert_eq!(get_data(&db, "User", json!({ "name": true, "$where": { "name": "Dan" } })), json!([]));
}
