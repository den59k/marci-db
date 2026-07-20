use marcidb::{MarciDB, UpdateError};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, try_update_many, update_many_data};

/// `score` is indexed, `plain` is not — so a filter can be pointed at either access path.
fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model Item {
      name    String
      score   UInt    @index
      plain   UInt
    }
  ";

  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  for score in [10u64, 20, 30, 40] {
    insert_data(&db, "Item", json!({ "name": format!("i{}", score), "score": score, "plain": score }));
  }

  db
}

fn scores(db: &MarciDB) -> serde_json::Value {
  get_data(db, "Item", json!({ "name": true, "score": true }))
}

#[test]
fn update_many_basic_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Filter on the unindexed field — a residual row scan
  let count = update_many_data(&db, "Item", json!({ "$where": { "plain": { "$gte": 30 } } }), json!({ "score": 0 }));
  assert_eq!(count, 2);

  assert_eq!(scores(&db), json!([
    { "name": "i10", "score": 10 },
    { "name": "i20", "score": 20 },
    { "name": "i30", "score": 0 },
    { "name": "i40", "score": 0 },
  ]));
}

/// The scan walks the `score` index while the update rewrites keys in that same index tree. canopydb
/// allows only one live handle per tree per write transaction, so this fails at runtime unless the
/// query context is dropped before the update loop begins.
#[test]
fn update_many_over_scanned_index_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let count = update_many_data(
    &db, "Item",
    json!({ "$where": { "score": { "$gte": 20 } } }),
    json!({ "score": { "$increment": 5 } }),
  );
  assert_eq!(count, 3);

  assert_eq!(scores(&db), json!([
    { "name": "i10", "score": 10 },
    { "name": "i20", "score": 25 },
    { "name": "i30", "score": 35 },
    { "name": "i40", "score": 45 },
  ]));

  // The index itself must still be consistent — query through it after the rewrite
  assert_eq!(
    get_data(&db, "Item", json!({ "name": true, "$where": { "score": { "$between": [25, 35] } } })),
    json!([{ "name": "i20" }, { "name": "i30" }])
  );
}

#[test]
fn update_many_count_and_no_match_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // No match — nothing updated
  assert_eq!(update_many_data(&db, "Item", json!({ "$where": { "score": { "$gt": 100 } } }), json!({ "plain": 1 })), 0);

  // No $where at all — every row matches
  assert_eq!(update_many_data(&db, "Item", json!({}), json!({ "plain": 7 })), 4);
  assert_eq!(
    get_data(&db, "Item", json!({ "plain": true })),
    json!([{ "plain": 7 }, { "plain": 7 }, { "plain": 7 }, { "plain": 7 }])
  );

  // The count is rows *matched*, not rows whose bytes changed — re-applying the same value still counts
  assert_eq!(update_many_data(&db, "Item", json!({}), json!({ "plain": 7 })), 4);
}

/// A failure part-way through must leave the whole operation uncommitted.
#[test]
fn update_many_rolls_back_on_error_test() {
  let dir = tempdir().unwrap();
  let db = MarciDB::new(
    "model User {\n  name  String\n  email String @unique\n}",
    dir.path().to_str().unwrap(),
  );

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  insert_data(&db, "User", json!({ "name": "Bob", "email": "b@x.com" }));

  // Collapsing both rows onto one unique email collides on the second row
  let err = try_update_many(&db, "User", json!({}), json!({ "email": "same@x.com" })).unwrap_err();
  assert!(matches!(err, UpdateError::UniqueViolation(_, _)), "unexpected error: {:?}", err);

  // Alice's successful update must have been rolled back too
  assert_eq!(
    get_data(&db, "User", json!({ "name": true, "email": true })),
    json!([
      { "name": "Alice", "email": "a@x.com" },
      { "name": "Bob", "email": "b@x.com" },
    ])
  );
}

#[test]
fn update_many_rejects_unsupported_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // A bounded update has no total order to bound against
  for bound in [json!({ "$limit": 2 }), json!({ "$skip": 1 })] {
    let mut query = bound.as_object().unwrap().clone();
    query.insert("$where".to_string(), json!({ "score": { "$gte": 20 } }));
    let err = try_update_many(&db, "Item", serde_json::Value::Object(query), json!({ "plain": 1 })).unwrap_err();
    assert!(matches!(err, UpdateError::Unsupported(_)), "unexpected error: {:?}", err);
  }

  // Nothing was written by the rejected calls
  assert_eq!(
    get_data(&db, "Item", json!({ "plain": true })),
    json!([{ "plain": 10 }, { "plain": 20 }, { "plain": 30 }, { "plain": 40 }])
  );
}

/// Two updates to the same model inside one transaction. This panicked inside canopydb before
/// `process_update` stopped writing back through the handle it had just read from — it affects plain
/// `$transaction` batches too, not just `update_many`.
#[test]
fn two_updates_in_one_transaction_test() {
  use marcidb::{parse_id, parse_update};
  let dir = tempdir().unwrap();
  let db = create_db(&dir);
  let entity = db.get_model("Item").unwrap();

  let a = insert_data(&db, "Item", json!({ "name": "p1", "score": 1, "plain": 1 }));
  let b = insert_data(&db, "Item", json!({ "name": "p2", "score": 2, "plain": 2 }));

  let tx = db.begin_write().unwrap();
  for idv in [&a, &b] {
    let id = parse_id(&db.schema, entity, idv).unwrap();
    let up = parse_update(&db.schema, entity, &json!({ "plain": 99 })).unwrap();
    assert!(tx.update_item(entity, &id, &up).unwrap());
  }
  tx.commit().unwrap();

  assert_eq!(
    get_data(&db, "Item", json!({ "plain": true, "$where": { "name": { "$startsWith": "p" } } })),
    json!([{ "plain": 99 }, { "plain": 99 }])
  );
}
