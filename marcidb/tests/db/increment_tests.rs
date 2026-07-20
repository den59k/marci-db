use marcidb::{MarciDB, UpdateError, parse_id, parse_update};
use serde_json::{Value, json};
use tempfile::tempdir;

use crate::db::{get_data, insert_data, try_update_many};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  MarciDB::new("
    model Acc {
      name   String
      i      Int
      u      UInt
      f      Float
      d      Double
      t      DateTime
    }
  ", dir.path().to_str().unwrap())
}

/// `update` without unwrapping, for the rejection cases.
fn try_update(db: &MarciDB, id: &Value, data: Value) -> Result<(), UpdateError> {
  let entity = db.get_model("Acc").unwrap();
  let id = parse_id(&db.schema, entity, id).unwrap();
  let op = parse_update(&db.schema, entity, &data).unwrap();
  db.update_item(entity, &id, &op)
}

fn row(db: &MarciDB, field: &str) -> Value {
  get_data(db, "Acc", json!({ field: true }))[0][field].clone()
}

/// A `UInt` field can now be decremented — the delta is signed even though the storage is not.
#[test]
fn decrement_unsigned_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);
  let id = insert_data(&db, "Acc", json!({ "name": "a", "i": 0, "u": 100, "f": 0.0, "d": 0.0, "t": 0 }));

  try_update(&db, &id, json!({ "u": { "$increment": -30 } })).unwrap();
  assert_eq!(row(&db, "u"), json!(70));

  // Down to exactly zero is in range
  try_update(&db, &id, json!({ "u": { "$increment": -70 } })).unwrap();
  assert_eq!(row(&db, "u"), json!(0));
}

/// Every numeric type increments and decrements normally away from its bounds.
#[test]
fn increment_all_types_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);
  let id = insert_data(&db, "Acc", json!({ "name": "a", "i": 10, "u": 10, "f": 1.5, "d": 1.5, "t": 1000 }));

  try_update(&db, &id, json!({ "i": { "$increment": -25 } })).unwrap();
  try_update(&db, &id, json!({ "u": { "$increment": 5 } })).unwrap();
  try_update(&db, &id, json!({ "f": { "$increment": 0.25 } })).unwrap();
  try_update(&db, &id, json!({ "d": { "$increment": -0.5 } })).unwrap();
  try_update(&db, &id, json!({ "t": { "$increment": 500 } })).unwrap();

  assert_eq!(row(&db, "i"), json!(-15));
  assert_eq!(row(&db, "u"), json!(15));
  assert_eq!(row(&db, "t"), json!(1500));
  // Compared numerically so the assertion doesn't depend on the encoder's chosen float spelling.
  assert_eq!(row(&db, "f").as_f64().unwrap(), 1.75);
  assert_eq!(row(&db, "d").as_f64().unwrap(), 1.0);
}

/// Both bounds of every numeric type are rejected rather than wrapped, and the row is left untouched.
/// Without the range check these silently wrapped in release builds and panicked in debug.
#[test]
fn increment_out_of_range_is_rejected_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let cases: Vec<(&str, Value, Value)> = vec![
    // field, starting value, out-of-range delta
    ("u", json!(10u64),           json!(-11)),                    // unsigned below zero
    ("u", json!(u64::MAX - 5),    json!(100)),                    // unsigned above max
    ("i", json!(i64::MAX - 5),    json!(100)),                    // signed above max
    ("i", json!(i64::MIN + 5),    json!(-100)),                   // signed below min
    ("t", json!(i64::MAX - 5),    json!(100)),                    // DateTime is i64 millis
    ("t", json!(i64::MIN + 5),    json!(-100)),
    ("f", json!(f32::MAX),        json!(f32::MAX)),               // float saturates to +inf
    ("f", json!(-f32::MAX),       json!(-f32::MAX)),
    ("d", json!(f64::MAX),        json!(f64::MAX)),               // double saturates to +inf
    ("d", json!(-f64::MAX),       json!(-f64::MAX)),
  ];

  for (field, start, delta) in cases {
    let mut data = json!({ "name": "a", "i": 0, "u": 0, "f": 0.0, "d": 0.0, "t": 0 });
    data[field] = start.clone();
    let id = insert_data(&db, "Acc", data);

    let err = try_update(&db, &id, json!({ field: { "$increment": delta } })).unwrap_err();
    assert!(
      matches!(err, UpdateError::IncrementOutOfRange(_)),
      "expected IncrementOutOfRange for {} {} += {}, got {:?}", field, start, delta, err
    );

    // "Unchanged" is checked by filtering on the original value rather than reading it back: a `Float`
    // round-trips through f32, so the decoded value is not literally equal to the f64 literal inserted
    // here, and comparing the two would fail for reasons that have nothing to do with the update.
    let still = get_data(&db, "Acc", json!({ "name": true, "$where": { field: start.clone() } }));
    assert_eq!(
      still.as_array().unwrap().len(), 1,
      "{} no longer equals {} after a rejected increment", field, start
    );
  }
}

/// A rejected increment aborts the whole `updateMany`, including rows that were fine on their own.
#[test]
fn increment_rejection_rolls_back_update_many_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  insert_data(&db, "Acc", json!({ "name": "ok",  "i": 0, "u": 100,        "f": 0.0, "d": 0.0, "t": 0 }));
  insert_data(&db, "Acc", json!({ "name": "bad", "i": 0, "u": 1,          "f": 0.0, "d": 0.0, "t": 0 }));

  // -50 is fine for the first row and underflows the second
  let err = try_update_many(&db, "Acc", json!({}), json!({ "u": { "$increment": -50 } })).unwrap_err();
  assert!(matches!(err, UpdateError::IncrementOutOfRange(_)), "got {:?}", err);

  // Neither row moved — not even the one whose own increment was in range
  assert_eq!(
    get_data(&db, "Acc", json!({ "name": true, "u": true })),
    json!([{ "name": "ok", "u": 100 }, { "name": "bad", "u": 1 }])
  );
}

/// The same rejection inside a batch rolls the batch back, so a preceding insert does not survive.
#[test]
fn increment_rejection_rolls_back_batch_test() {
  use marcidb::execute_batch;
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  insert_data(&db, "Acc", json!({ "name": "a", "i": 0, "u": 1, "f": 0.0, "d": 0.0, "t": 0 }));

  let res = execute_batch(&db, &[
    json!({ "model": "Acc", "action": "insert",
            "data": { "name": "added", "i": 0, "u": 0, "f": 0.0, "d": 0.0, "t": 0 } }),
    json!({ "model": "Acc", "action": "updateMany",
            "query": { "$where": { "name": "a" } }, "data": { "u": { "$increment": -5 } } }),
  ]);
  assert!(res.is_err(), "batch should fail on the out-of-range increment");

  // The insert from op 0 was rolled back with the batch
  assert_eq!(
    get_data(&db, "Acc", json!({ "name": true, "u": true })),
    json!([{ "name": "a", "u": 1 }])
  );
}

/// `$increment` on a field that is currently null stays a no-op — the range check must not change that.
#[test]
fn increment_null_is_still_a_noop_test() {
  let dir = tempdir().unwrap();
  let db = MarciDB::new("
    model Acc {
      name   String
      u      UInt?
    }
  ", dir.path().to_str().unwrap());

  insert_data(&db, "Acc", json!({ "name": "null", "u": null }));
  let n = try_update_many(&db, "Acc", json!({}), json!({ "u": { "$increment": -5 } })).unwrap();
  assert_eq!(n, 1);
  assert_eq!(get_data(&db, "Acc", json!({ "u": true })), json!([{ "u": null }]));
}
