//! `@unique(<name>)` on several fields: one composite unique index — one like per user and post, one
//! booking per master and slot. Enforced on insert and update, released on delete and when a part goes
//! null, built by a migration from the rows that exist (which must not collide), carried by the snapshot.
use marcidb::{InsertError, MarciDB, MigrateApplyError, UpdateError, parse_snapshot, serialize_snapshot, try_parse_schema};
use marcidb_schema::{diff, evolve, migration_ops, reconcile, serialize_migration};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{delete_data, get_data, insert_data, try_insert, try_update, update_data};

const SCHEMA: &str = "
    model User {
        name    String
    }

    model Post {
        title   String
    }

    model Like {
        user    User    @unique(pair)
        post    Post    @unique(pair)
        kind    String
    }

    model Booking {
        master  String  @unique(slot)
        at      Int?    @unique(slot)
        client  String
    }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

fn migrate_to(db: &mut MarciDB, schema_text: &str) -> Result<(), MigrateApplyError> {
    let mut new_schema = try_parse_schema(schema_text)?;
    reconcile(&mut new_schema, &db.schema);
    let ops = diff(&db.schema, &new_schema)?;
    db.commit_schema(new_schema, &ops)
}

#[test]
fn a_pair_is_taken_once() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob = insert_data(&db, "User", json!({ "name": "Bob" }));
    let p1 = insert_data(&db, "Post", json!({ "title": "One" }));
    let p2 = insert_data(&db, "Post", json!({ "title": "Two" }));

    let first = insert_data(&db, "Like", json!({ "user": alice.clone(), "post": p1.clone(), "kind": "heart" }));
    let e = try_insert(&db, "Like", json!({ "user": alice.clone(), "post": p1.clone(), "kind": "fire" })).unwrap_err();
    assert_eq!(e.to_string(), "Like.user, Like.post: the value is already taken (by the row with id 1)");
    assert_eq!((e.kind(), e.field()), ("unique", Some("Like.user, Like.post")));
    assert!(matches!(e, InsertError::UniqueViolation(..)));

    // the same user on another post, another user on the same post: fine
    insert_data(&db, "Like", json!({ "user": alice.clone(), "post": p2.clone(), "kind": "heart" }));
    let bobs = insert_data(&db, "Like", json!({ "user": bob.clone(), "post": p1.clone(), "kind": "heart" }));
    assert_eq!(get_data(&db, "Like", json!({ "kind": true })).as_array().unwrap().len(), 3);

    // an update onto a taken pair is refused; onto a free one moves the entry, and the old pair is free again
    let e = try_update(&db, "Like", &bobs, json!({ "user": { "$connect": alice.clone() } })).unwrap_err();
    assert_eq!(e.to_string(), "Like.user, Like.post: the value is already taken (by the row with id 1)");
    assert!(matches!(e, UpdateError::UniqueViolation(..)));
    update_data(&db, "Like", &bobs, json!({ "post": { "$connect": p2.clone() } }));
    insert_data(&db, "Like", json!({ "user": bob.clone(), "post": p1.clone(), "kind": "fire" }));
    let e = try_insert(&db, "Like", json!({ "user": bob.clone(), "post": p2.clone(), "kind": "fire" })).unwrap_err();
    assert_eq!(e.kind(), "unique");

    // a delete releases the pair; a field outside the group changes freely
    delete_data(&db, "Like", first.clone());
    let again = insert_data(&db, "Like", json!({ "user": alice.clone(), "post": p1.clone(), "kind": "fire" }));
    update_data(&db, "Like", &again, json!({ "kind": "heart" }));
    assert_eq!(get_data(&db, "Like", json!({ "kind": true, "$where": { "user": alice.clone(), "post": p1 } })), json!([{ "kind": "heart" }]));
}

#[test]
fn a_null_part_is_not_constrained() {
    let (_dir, db) = make_db();
    insert_data(&db, "Booking", json!({ "master": "Ann", "client": "a" }));
    insert_data(&db, "Booking", json!({ "master": "Ann", "client": "b" }));
    let ten = insert_data(&db, "Booking", json!({ "master": "Ann", "at": 10, "client": "c" }));
    let e = try_insert(&db, "Booking", json!({ "master": "Ann", "at": 10, "client": "d" })).unwrap_err();
    assert_eq!(e.to_string(), "Booking.master, Booking.at: the value is already taken (by the row with id 3)");
    insert_data(&db, "Booking", json!({ "master": "Bea", "at": 10, "client": "d" }));

    // the slot goes null: released; set again: taken again
    update_data(&db, "Booking", &ten, json!({ "at": null }));
    let other = insert_data(&db, "Booking", json!({ "master": "Ann", "at": 10, "client": "e" }));
    let e = try_update(&db, "Booking", &ten, json!({ "at": 10 })).unwrap_err();
    assert_eq!(e.kind(), "unique");
    update_data(&db, "Booking", &other, json!({ "at": 11 }));
    update_data(&db, "Booking", &ten, json!({ "at": 10 }));
}

#[test]
fn a_group_of_one_field_is_refused() {
    let e = try_parse_schema("model Like {\n  user String @unique(pair)\n  post String\n}").unwrap_err();
    assert!(e.to_string().contains("two or more fields"), "{}", e);
    let e = try_parse_schema("model Like {\n  tags String[] @unique(pair)\n  post String @unique(pair)\n}").unwrap_err();
    assert!(e.to_string().contains("list"), "{}", e);
}

const WITHOUT: &str = "model Like {\n  user String\n  post String\n}";
const WITH: &str = "model Like {\n  user String @unique(pair)\n  post String @unique(pair)\n}";

#[test]
fn a_migration_builds_the_index_from_the_rows_and_a_reopen_keeps_it() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let mut db = MarciDB::new(WITHOUT, &path);
        insert_data(&db, "Like", json!({ "user": "a", "post": "1" }));
        let dup = insert_data(&db, "Like", json!({ "user": "a", "post": "1" }));
        insert_data(&db, "Like", json!({ "user": "a", "post": "2" }));

        // the rows collide: refused, nothing changes
        match migrate_to(&mut db, WITH) {
            Err(MigrateApplyError::UniqueViolation { field }) => assert_eq!(field, "Like.user, Like.post"),
            other => panic!("expected a unique violation, got {:?}", other),
        }
        let extra = insert_data(&db, "Like", json!({ "user": "a", "post": "1" }));

        delete_data(&db, "Like", dup);
        delete_data(&db, "Like", extra);
        migrate_to(&mut db, WITH).unwrap();
        let e = try_insert(&db, "Like", json!({ "user": "a", "post": "1" })).unwrap_err();
        assert_eq!(e.kind(), "unique");
        let b1 = insert_data(&db, "Like", json!({ "user": "b", "post": "1" }));
        std::fs::write(dir.path().join("b1"), b1["id"].to_string()).unwrap();

        // the snapshot carries the attribute
        let snap = serialize_snapshot(&db.schema);
        assert!(snap.contains("@unique(pair)"), "{}", snap);
        assert_eq!(parse_snapshot(&snap).unwrap().models[0].unique_groups.len(), 1);
    }
    let mut db = MarciDB::open(&path);
    assert_eq!(db.schema.models[0].unique_groups.len(), 1);
    let e = try_insert(&db, "Like", json!({ "user": "b", "post": "1" })).unwrap_err();
    let b1 = std::fs::read_to_string(dir.path().join("b1")).unwrap();
    assert_eq!(e.to_string(), format!("Like.user, Like.post: the value is already taken (by the row with id {})", b1));

    // dropping the group frees the pair
    migrate_to(&mut db, WITHOUT).unwrap();
    insert_data(&db, "Like", json!({ "user": "b", "post": "1" }));
}

#[test]
fn the_migration_file_carries_the_group() {
    let prev = marcidb::parse_schema(WITHOUT);
    let mut next = marcidb::parse_schema(WITH);
    reconcile(&mut next, &prev);
    let text = serialize_migration(&diff(&prev, &next).unwrap(), &next);
    assert_eq!(text, "add unique_group Like.pair(user, post)");

    // laid onto the snapshot, the group is back; and the reverse drops it
    let evolved = evolve(&serialize_snapshot(&prev), &text).unwrap();
    assert_eq!(parse_snapshot(&evolved).unwrap().models[0].unique_groups[0].name, "pair");
    assert!(matches!(migration_ops(&text).unwrap()[0], marcidb_schema::MigrateOp::AddUniqueGroup { .. }));
    let back = serialize_migration(&diff(&next, &prev).unwrap(), &prev);
    assert_eq!(back, "drop unique_group Like.pair");
    let evolved = evolve(&evolved, &back).unwrap();
    assert!(parse_snapshot(&evolved).unwrap().models[0].unique_groups.is_empty());
}
