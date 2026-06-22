use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data, update_data};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model Doc {
      name        String
      data        Json
      meta        Json?
    }
  ";
  MarciDB::new(schema_str, dir.path().to_str().unwrap())
}

/// Insert a spread of JSON shapes (object, array, nested, every scalar, an explicit nested null) and read
/// them back through the full storage path. Object comparison is order-independent (serde_json `Value`),
/// so the canonical sorted-key encoding doesn't affect equality.
#[test]
fn json_round_trip_through_db() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  insert_data(&db, "Doc", json!({
    "name": "obj",
    "data": { "city": "Tokyo", "tags": ["a", "b"], "active": true, "score": 4.5, "note": null },
    "meta": { "v": 1 }
  }));
  insert_data(&db, "Doc", json!({
    "name": "arr",
    "data": [1, "two", false, null, { "nested": [3, 4] }]
  }));
  insert_data(&db, "Doc", json!({
    "name": "scalar",
    "data": "just a string"
  }));

  let resp = get_data(&db, "Doc", json!({ "name": true, "data": true, "meta": true }));
  assert_eq!(resp, json!([
    { "name": "obj",    "data": { "city": "Tokyo", "tags": ["a", "b"], "active": true, "score": 4.5, "note": null }, "meta": { "v": 1 } },
    // An omitted nullable Json field reads back as null (offset 0), like every other type.
    { "name": "arr",    "data": [1, "two", false, null, { "nested": [3, 4] }], "meta": null },
    { "name": "scalar", "data": "just a string", "meta": null },
  ]));
}

/// Selecting only the JSON field returns just that field, and a JSON field can be overwritten by an update
/// (it is variable-length, so the row is rewritten — same path as `String`).
#[test]
fn json_select_only_and_update() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let id = insert_data(&db, "Doc", json!({ "name": "x", "data": { "a": 1 } }));

  let only = get_data_one(&db, "Doc", json!({ "data": true, "$where": { "name": "x" } }));
  assert_eq!(only, json!({ "data": { "a": 1 } }));

  update_data(&db, "Doc", &id, json!({ "data": { "a": 2, "b": [true, false] } }));
  let after = get_data_one(&db, "Doc", json!({ "name": true, "data": true, "$where": { "name": "x" } }));
  assert_eq!(after, json!({ "name": "x", "data": { "a": 2, "b": [true, false] } }));
}

/// JSON survives reopening the database. `MarciDB::open` reconstructs the schema from the stored snapshot,
/// so this also exercises the snapshot round-trip of the `Json` type (serialize/parse "Json").
#[test]
fn json_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let payload = json!({ "k": [1, 2, { "deep": "value" }], "flag": true });

  {
    let db = create_db(&dir);
    insert_data(&db, "Doc", json!({ "name": "persist", "data": payload }));
  }

  let db = MarciDB::open(dir.path().to_str().unwrap());
  let resp = get_data_one(&db, "Doc", json!({ "data": true, "$where": { "name": "persist" } }));
  assert_eq!(resp, json!({ "data": { "k": [1, 2, { "deep": "value" }], "flag": true } }));
}
